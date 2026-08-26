use crate::pool::Pool;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Color {
    Red,
    Black,
}

#[derive(Debug, Clone)]
struct Node<K, V> {
    color: Color,
    parent: u32,
    left: u32,
    right: u32,
    key: K,
    value: V,
}

#[derive(Debug, Clone)]
pub struct RbTree<K, V> {
    nodes: Pool<Node<K, V>>,
}

impl<K, V> RbTree<K, V>
where
    K: Ord + Copy + Default,
    V: Default,
{
    #[inline]
    pub fn new() -> Self {
        Self::with_capacity(16)
    }

    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        let mut nodes = Pool::with_capacity(capacity.saturating_add(1).max(1));
        let nil = Node {
            color: Color::Black,
            parent: 0,
            left: 0,
            right: 0,
            key: K::default(),
            value: V::default(),
        };
        nodes.insert(nil);
        nodes.set_opaque(0);
        Self { nodes }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.nodes.len().saturating_sub(1)
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.root() == 0
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.nodes.capacity().saturating_sub(1)
    }

    #[inline]
    pub fn contains_key(&self, key: &K) -> bool {
        self.find_node(*key).is_some()
    }

    #[inline]
    pub fn prefetch_first(&self) {
        let root = self.root();
        if root != 0 {
            let node = self.node(root);
            crate::prefetch::prefetch_read_l1(node as *const Node<K, V>);
        }
    }

    #[inline]
    pub fn prefetch_node(&self, key: &K) {
        let mut cursor = self.root();
        while cursor != 0 {
            let node = self.node(cursor);
            match key.cmp(&node.key) {
                core::cmp::Ordering::Less => {
                    let left = node.left;
                    if left != 0 {
                        let next = self.node(left);
                        crate::prefetch::prefetch_read_l1(next as *const Node<K, V>);
                    }
                    cursor = left;
                }
                core::cmp::Ordering::Greater => {
                    let right = node.right;
                    if right != 0 {
                        let next = self.node(right);
                        crate::prefetch::prefetch_read_l1(next as *const Node<K, V>);
                    }
                    cursor = right;
                }
                core::cmp::Ordering::Equal => return,
            }
        }
    }

    #[inline]
    pub fn get(&self, key: &K) -> Option<&V> {
        let index = self.find_node(*key)?;
        Some(&self.node(index).value)
    }

    #[inline]
    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        let index = self.find_node(*key)?;
        Some(&mut self.node_mut(index).value)
    }

    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        let mut parent = 0;
        let mut cursor = self.root();
        while cursor != 0 {
            parent = cursor;
            match key.cmp(&self.node(cursor).key) {
                core::cmp::Ordering::Less => cursor = self.node(cursor).left,
                core::cmp::Ordering::Greater => cursor = self.node(cursor).right,
                core::cmp::Ordering::Equal => {
                    let node = self.node_mut(cursor);
                    return Some(core::mem::replace(&mut node.value, value));
                }
            }
        }

        let node_index = self.nodes.insert(Node {
            color: Color::Red,
            parent,
            left: 0,
            right: 0,
            key,
            value,
        });
        if parent == 0 {
            self.set_root(node_index);
        } else if key < self.node(parent).key {
            self.node_mut(parent).left = node_index;
        } else {
            self.node_mut(parent).right = node_index;
        }
        self.insert_fixup(node_index);
        None
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        let Some(z) = self.find_node(*key) else {
            return None;
        };

        let mut y = z;
        let mut y_original_color = self.node(y).color;
        let x;
        if self.node(z).left == 0 {
            x = self.node(z).right;
            self.transplant(z, x);
        } else if self.node(z).right == 0 {
            x = self.node(z).left;
            self.transplant(z, x);
        } else {
            y = self.minimum_index(self.node(z).right);
            y_original_color = self.node(y).color;
            x = self.node(y).right;
            if self.node(y).parent == z {
                self.node_mut(x).parent = y;
            } else {
                self.transplant(y, self.node(y).right);
                let z_right = self.node(z).right;
                self.node_mut(y).right = z_right;
                self.node_mut(z_right).parent = y;
            }
            self.transplant(z, y);
            let z_left = self.node(z).left;
            self.node_mut(y).left = z_left;
            self.node_mut(z_left).parent = y;
            let z_color = self.node(z).color;
            self.node_mut(y).color = z_color;
        }

        if y_original_color == Color::Black {
            self.delete_fixup(x);
        }

        let removed = self.nodes.remove(z);
        Some(removed?.value)
    }

    #[inline]
    pub fn first(&self) -> Option<(&K, &V)> {
        let root = self.root();
        if root == 0 {
            return None;
        }
        let node = self.node(self.minimum_index(root));
        Some((&node.key, &node.value))
    }

    #[inline]
    pub fn last(&self) -> Option<(&K, &V)> {
        let root = self.root();
        if root == 0 {
            return None;
        }
        let node = self.node(self.maximum_index(root));
        Some((&node.key, &node.value))
    }

    #[inline]
    pub fn predecessor(&self, key: &K) -> Option<(&K, &V)> {
        let index = self.predecessor_index(*key)?;
        let node = self.node(index);
        Some((&node.key, &node.value))
    }

    #[inline]
    pub fn successor(&self, key: &K) -> Option<(&K, &V)> {
        let index = self.successor_index(*key)?;
        let node = self.node(index);
        Some((&node.key, &node.value))
    }

    #[inline]
    pub fn iter(&self) -> Iter<'_, K, V> {
        let mut stack = Vec::new();
        let mut cursor = self.root();
        while cursor != 0 {
            let node = self.node(cursor);
            stack.push(cursor);
            cursor = node.left;
        }
        Iter { tree: self, stack }
    }

    #[inline]
    fn root(&self) -> u32 {
        self.nodes.opaque()
    }

    #[inline]
    fn set_root(&mut self, root: u32) {
        self.nodes.set_opaque(root);
    }

    #[inline]
    fn node(&self, index: u32) -> &Node<K, V> {
        self.nodes
            .get(index)
            .expect("RB-tree node index is occupied")
    }

    #[inline]
    fn node_mut(&mut self, index: u32) -> &mut Node<K, V> {
        self.nodes
            .get_mut(index)
            .expect("RB-tree node index is occupied")
    }

    #[inline]
    fn find_node(&self, key: K) -> Option<u32> {
        let mut cursor = self.root();
        while cursor != 0 {
            let node = self.node(cursor);
            match key.cmp(&node.key) {
                core::cmp::Ordering::Less => cursor = node.left,
                core::cmp::Ordering::Greater => cursor = node.right,
                core::cmp::Ordering::Equal => return Some(cursor),
            }
        }
        None
    }

    #[inline]
    fn minimum_index(&self, mut index: u32) -> u32 {
        while self.node(index).left != 0 {
            index = self.node(index).left;
        }
        index
    }

    #[inline]
    fn maximum_index(&self, mut index: u32) -> u32 {
        while self.node(index).right != 0 {
            index = self.node(index).right;
        }
        index
    }

    #[inline]
    fn predecessor_index(&self, key: K) -> Option<u32> {
        let mut cursor = self.root();
        let mut candidate = 0;
        while cursor != 0 {
            let node = self.node(cursor);
            if key <= node.key {
                cursor = node.left;
            } else {
                candidate = cursor;
                cursor = node.right;
            }
        }
        (candidate != 0).then_some(candidate)
    }

    #[inline]
    fn successor_index(&self, key: K) -> Option<u32> {
        let mut cursor = self.root();
        let mut candidate = 0;
        while cursor != 0 {
            let node = self.node(cursor);
            if key < node.key {
                candidate = cursor;
                cursor = node.left;
            } else {
                cursor = node.right;
            }
        }
        (candidate != 0).then_some(candidate)
    }

    fn rotate_left(&mut self, x: u32) {
        let y = self.node(x).right;
        let y_left = self.node(y).left;
        self.node_mut(x).right = y_left;
        self.node_mut(y_left).parent = x;

        let x_parent = self.node(x).parent;
        self.node_mut(y).parent = x_parent;
        if x_parent == 0 {
            self.set_root(y);
        } else if self.node(x_parent).left == x {
            self.node_mut(x_parent).left = y;
        } else {
            self.node_mut(x_parent).right = y;
        }
        self.node_mut(y).left = x;
        self.node_mut(x).parent = y;
    }

    fn rotate_right(&mut self, x: u32) {
        let y = self.node(x).left;
        let y_right = self.node(y).right;
        self.node_mut(x).left = y_right;
        self.node_mut(y_right).parent = x;

        let x_parent = self.node(x).parent;
        self.node_mut(y).parent = x_parent;
        if x_parent == 0 {
            self.set_root(y);
        } else if self.node(x_parent).right == x {
            self.node_mut(x_parent).right = y;
        } else {
            self.node_mut(x_parent).left = y;
        }
        self.node_mut(y).right = x;
        self.node_mut(x).parent = y;
    }

    fn insert_fixup(&mut self, mut z: u32) {
        while self.node(self.node(z).parent).color == Color::Red {
            let parent = self.node(z).parent;
            let grandparent = self.node(parent).parent;
            if self.node(grandparent).left == parent {
                let uncle = self.node(grandparent).right;
                if self.color_of(uncle) == Color::Red {
                    self.node_mut(parent).color = Color::Black;
                    self.node_mut(uncle).color = Color::Black;
                    self.node_mut(grandparent).color = Color::Red;
                    z = grandparent;
                } else {
                    if self.node(parent).right == z {
                        z = parent;
                        self.rotate_left(z);
                    }
                    let parent = self.node(z).parent;
                    let grandparent = self.node(parent).parent;
                    self.node_mut(parent).color = Color::Black;
                    self.node_mut(grandparent).color = Color::Red;
                    self.rotate_right(grandparent);
                }
            } else {
                let uncle = self.node(grandparent).left;
                if self.color_of(uncle) == Color::Red {
                    self.node_mut(parent).color = Color::Black;
                    self.node_mut(uncle).color = Color::Black;
                    self.node_mut(grandparent).color = Color::Red;
                    z = grandparent;
                } else {
                    if self.node(parent).left == z {
                        z = parent;
                        self.rotate_right(z);
                    }
                    let parent = self.node(z).parent;
                    let grandparent = self.node(parent).parent;
                    self.node_mut(parent).color = Color::Black;
                    self.node_mut(grandparent).color = Color::Red;
                    self.rotate_left(grandparent);
                }
            }
        }
        let root = self.root();
        self.node_mut(root).color = Color::Black;
    }

    fn transplant(&mut self, u: u32, v: u32) {
        let parent = self.node(u).parent;
        if parent == 0 {
            self.set_root(v);
        } else if self.node(parent).left == u {
            self.node_mut(parent).left = v;
        } else {
            self.node_mut(parent).right = v;
        }
        self.node_mut(v).parent = parent;
    }

    fn delete_fixup(&mut self, mut x: u32) {
        while x != self.root() && self.color_of(x) == Color::Black {
            let parent = self.node(x).parent;
            if self.node(parent).left == x {
                let mut sibling = self.node(parent).right;
                if self.color_of(sibling) == Color::Red {
                    self.node_mut(sibling).color = Color::Black;
                    self.node_mut(parent).color = Color::Red;
                    self.rotate_left(parent);
                    sibling = self.node(parent).right;
                }
                let sibling_left = self.node(sibling).left;
                let sibling_right = self.node(sibling).right;
                if self.color_of(sibling_left) == Color::Black
                    && self.color_of(sibling_right) == Color::Black
                {
                    if sibling != 0 {
                        self.node_mut(sibling).color = Color::Red;
                    }
                    x = parent;
                } else {
                    if self.color_of(sibling_right) == Color::Black {
                        self.node_mut(sibling_left).color = Color::Black;
                        self.node_mut(sibling).color = Color::Red;
                        self.rotate_right(sibling);
                        sibling = self.node(parent).right;
                    }
                    let parent_color = self.node(parent).color;
                    self.node_mut(sibling).color = parent_color;
                    self.node_mut(parent).color = Color::Black;
                    let sibling_right = self.node(sibling).right;
                    self.node_mut(sibling_right).color = Color::Black;
                    self.rotate_left(parent);
                    x = self.root();
                }
            } else {
                let mut sibling = self.node(parent).left;
                if self.color_of(sibling) == Color::Red {
                    self.node_mut(sibling).color = Color::Black;
                    self.node_mut(parent).color = Color::Red;
                    self.rotate_right(parent);
                    sibling = self.node(parent).left;
                }
                let sibling_right = self.node(sibling).right;
                let sibling_left = self.node(sibling).left;
                if self.color_of(sibling_right) == Color::Black
                    && self.color_of(sibling_left) == Color::Black
                {
                    if sibling != 0 {
                        self.node_mut(sibling).color = Color::Red;
                    }
                    x = parent;
                } else {
                    if self.color_of(sibling_left) == Color::Black {
                        self.node_mut(sibling_right).color = Color::Black;
                        self.node_mut(sibling).color = Color::Red;
                        self.rotate_left(sibling);
                        sibling = self.node(parent).left;
                    }
                    let parent_color = self.node(parent).color;
                    self.node_mut(sibling).color = parent_color;
                    self.node_mut(parent).color = Color::Black;
                    let sibling_left = self.node(sibling).left;
                    self.node_mut(sibling_left).color = Color::Black;
                    self.rotate_right(parent);
                    x = self.root();
                }
            }
        }
        self.node_mut(x).color = Color::Black;
    }

    #[inline]
    fn color_of(&self, index: u32) -> Color {
        self.node(index).color
    }
}

impl<K, V> Default for RbTree<K, V>
where
    K: Ord + Copy + Default,
    V: Default,
{
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

pub struct Iter<'a, K, V>
where
    K: Ord + Copy + Default,
    V: Default,
{
    tree: &'a RbTree<K, V>,
    stack: Vec<u32>,
}

impl<'a, K, V> Iterator for Iter<'a, K, V>
where
    K: Ord + Copy + Default,
    V: Default,
{
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        let index = self.stack.pop()?;
        let node = self.tree.node(index);
        let mut cursor = node.right;
        while cursor != 0 {
            self.stack.push(cursor);
            cursor = self.tree.node(cursor).left;
        }
        Some((&node.key, &node.value))
    }
}

#[cfg(test)]
mod tests {
    use super::RbTree;

    #[test]
    fn rbtree_insert_keeps_keys_sorted_for_iteration() {
        let mut tree = RbTree::new();
        assert_eq!(tree.insert(20u32, "b"), None);
        assert_eq!(tree.insert(10u32, "a"), None);
        assert_eq!(tree.insert(30u32, "c"), None);

        let items: std::vec::Vec<_> = tree.iter().map(|(k, v)| (*k, *v)).collect();
        assert_eq!(items, vec![(10, "a"), (20, "b"), (30, "c")]);
    }

    #[test]
    fn rbtree_predecessor_and_successor_follow_in_order_neighbors() {
        let mut tree = RbTree::new();
        assert_eq!(tree.insert(10u32, "a"), None);
        assert_eq!(tree.insert(20u32, "b"), None);
        assert_eq!(tree.insert(30u32, "c"), None);

        assert_eq!(tree.predecessor(&20).map(|(k, _)| *k), Some(10));
        assert_eq!(tree.successor(&20).map(|(k, _)| *k), Some(30));
    }

    #[test]
    fn rbtree_overwrite_existing_key_returns_old_value_without_duplicate_node() {
        let mut tree = RbTree::new();
        assert_eq!(tree.insert(10u32, "a"), None);
        assert_eq!(tree.insert(10u32, "b"), Some("a"));
        assert_eq!(tree.len(), 1);
        assert_eq!(tree.get(&10), Some(&"b"));
    }

    #[test]
    fn rbtree_search_neighbors_work_when_exact_key_is_missing() {
        let mut tree = RbTree::new();
        assert_eq!(tree.insert(100u32, "a"), None);
        assert_eq!(tree.insert(200u32, "b"), None);
        assert_eq!(tree.insert(300u32, "c"), None);

        assert_eq!(tree.predecessor(&250).map(|(k, _)| *k), Some(200));
        assert_eq!(tree.successor(&250).map(|(k, _)| *k), Some(300));
    }

    #[test]
    fn rbtree_remove_preserves_remaining_order() {
        let mut tree = RbTree::new();
        assert_eq!(tree.insert(10u32, "a"), None);
        assert_eq!(tree.insert(20u32, "b"), None);
        assert_eq!(tree.insert(30u32, "c"), None);

        assert_eq!(tree.remove(&20), Some("b"));
        let items: std::vec::Vec<_> = tree.iter().map(|(k, v)| (*k, *v)).collect();
        assert_eq!(items, vec![(10, "a"), (30, "c")]);
    }

    #[test]
    fn rbtree_first_last_and_get_mut_track_extreme_and_in_place_updates() {
        let mut tree = RbTree::new();
        assert_eq!(tree.insert(20u32, 2u32), None);
        assert_eq!(tree.insert(10u32, 1u32), None);
        assert_eq!(tree.insert(30u32, 3u32), None);

        assert_eq!(tree.first().map(|(k, v)| (*k, *v)), Some((10, 1)));
        assert_eq!(tree.last().map(|(k, v)| (*k, *v)), Some((30, 3)));

        let value = tree.get_mut(&20).expect("existing key");
        *value = 22;

        assert_eq!(tree.get(&20), Some(&22));
    }

    #[test]
    fn prefetch_first_does_not_panic_on_empty() {
        let tree: RbTree<u32, u32> = RbTree::new();
        tree.prefetch_first();
        tree.prefetch_node(&u32::MAX);
    }

    #[test]
    fn prefetch_first_and_node_do_not_mutate_tree() {
        let mut tree = RbTree::new();
        assert_eq!(tree.insert(10u32, "a"), None);
        assert_eq!(tree.insert(20u32, "b"), None);
        assert_eq!(tree.insert(30u32, "c"), None);
        tree.prefetch_first();
        tree.prefetch_node(&20);
        tree.prefetch_node(&999);
        assert_eq!(tree.len(), 3);
        assert_eq!(tree.get(&20), Some(&"b"));
    }

    #[test]
    fn rbtree_grows_beyond_initial_capacity_and_reuses_released_index() {
        let mut tree = RbTree::with_capacity(1);
        assert_eq!(tree.insert(20u32, 20u32), None);
        assert_eq!(tree.insert(10u32, 10u32), None);
        assert_eq!(tree.remove(&20), Some(20));
        assert_eq!(tree.insert(30u32, 30u32), None);

        assert_eq!(
            tree.iter().map(|(key, _)| *key).collect::<Vec<_>>(),
            vec![10, 30]
        );
    }
}
