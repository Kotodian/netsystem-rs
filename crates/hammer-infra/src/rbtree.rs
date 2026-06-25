use crate::pool::{Index as PoolIndex, Pool};
use crate::vec::Vec;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Color {
    Red,
    Black,
}

#[derive(Debug)]
struct Node<K, V> {
    key: K,
    value: V,
    color: Color,
    parent: Option<PoolIndex>,
    left: Option<PoolIndex>,
    right: Option<PoolIndex>,
}

#[derive(Debug)]
pub struct RbTree<K, V>
where
    K: Ord + Copy,
{
    root: Option<PoolIndex>,
    nodes: Pool<Node<K, V>>,
    len: usize,
}

impl<K, V> RbTree<K, V>
where
    K: Ord + Copy,
{
    #[inline]
    pub fn new() -> Self {
        Self {
            root: None,
            nodes: Pool::with_capacity(16),
            len: 0,
        }
    }

    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            root: None,
            nodes: Pool::with_capacity(capacity.max(1)),
            len: 0,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    pub fn contains_key(&self, key: &K) -> bool {
        self.find_node(*key).is_some()
    }

    #[inline]
    pub fn prefetch_first(&self) {
        let Some(root) = self.root else {
            return;
        };
        if let Some(ptr) = self.nodes.slot_ptr(root) {
            crate::prefetch::prefetch_read_l1(ptr);
        }
    }

    #[inline]
    pub fn prefetch_node(&self, key: &K) {
        let mut cursor = self.root;
        while let Some(index) = cursor {
            let node = self.node(index);
            match key.cmp(&node.key) {
                core::cmp::Ordering::Less => {
                    let left = node.left;
                    if let Some(child) = left
                        && let Some(ptr) = self.nodes.slot_ptr(child)
                    {
                        crate::prefetch::prefetch_read_l1(ptr);
                    }
                    cursor = left;
                }
                core::cmp::Ordering::Greater => {
                    let right = node.right;
                    if let Some(child) = right
                        && let Some(ptr) = self.nodes.slot_ptr(child)
                    {
                        crate::prefetch::prefetch_read_l1(ptr);
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
        let mut parent = None;
        let mut cursor = self.root;
        while let Some(index) = cursor {
            parent = Some(index);
            match key.cmp(&self.node(index).key) {
                core::cmp::Ordering::Less => cursor = self.node(index).left,
                core::cmp::Ordering::Greater => cursor = self.node(index).right,
                core::cmp::Ordering::Equal => {
                    let node = self.node_mut(index);
                    return Some(core::mem::replace(&mut node.value, value));
                }
            }
        }

        let node_index = self
            .nodes
            .insert(Node {
                key,
                value,
                color: Color::Red,
                parent,
                left: None,
                right: None,
            })
            .expect("rbtree node pool exhausted");
        if let Some(parent_index) = parent {
            if key < self.node(parent_index).key {
                self.node_mut(parent_index).left = Some(node_index);
            } else {
                self.node_mut(parent_index).right = Some(node_index);
            }
        } else {
            self.root = Some(node_index);
        }
        self.insert_fixup(node_index);
        self.len += 1;
        None
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        let z = self.find_node(*key)?;
        let mut y = z;
        let mut y_original_color = self.node(y).color;
        let x;
        let x_parent;

        if self.node(z).left.is_none() {
            x = self.node(z).right;
            x_parent = self.node(z).parent;
            self.transplant(z, self.node(z).right);
        } else if self.node(z).right.is_none() {
            x = self.node(z).left;
            x_parent = self.node(z).parent;
            self.transplant(z, self.node(z).left);
        } else {
            y = self.minimum_index(self.node(z).right.expect("right exists"));
            y_original_color = self.node(y).color;
            x = self.node(y).right;
            if self.node(y).parent == Some(z) {
                x_parent = Some(y);
                if let Some(x_index) = x {
                    self.node_mut(x_index).parent = Some(y);
                }
            } else {
                x_parent = self.node(y).parent;
                self.transplant(y, self.node(y).right);
                let z_right = self.node(z).right;
                self.node_mut(y).right = z_right;
                if let Some(right) = z_right {
                    self.node_mut(right).parent = Some(y);
                }
            }
            self.transplant(z, Some(y));
            let z_left = self.node(z).left;
            let z_color = self.node(z).color;
            {
                let y_node = self.node_mut(y);
                y_node.left = z_left;
                y_node.color = z_color;
            }
            if let Some(left) = z_left {
                self.node_mut(left).parent = Some(y);
            }
        }

        if y_original_color == Color::Black {
            self.delete_fixup(x, x_parent);
        }

        self.len -= 1;
        let removed = self.nodes.remove(z).expect("rbtree node is valid");
        Some(removed.value)
    }

    #[inline]
    pub fn first(&self) -> Option<(&K, &V)> {
        let root = self.root?;
        let index = self.minimum_index(root);
        let node = self.node(index);
        Some((&node.key, &node.value))
    }

    #[inline]
    pub fn last(&self) -> Option<(&K, &V)> {
        let root = self.root?;
        let index = self.maximum_index(root);
        let node = self.node(index);
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
        let mut cursor = self.root;
        while let Some(index) = cursor {
            stack.push(index);
            cursor = self.node(index).left;
        }
        Iter { tree: self, stack }
    }

    fn node(&self, index: PoolIndex) -> &Node<K, V> {
        self.nodes.get(index).expect("rbtree node index is valid")
    }

    fn node_mut(&mut self, index: PoolIndex) -> &mut Node<K, V> {
        self.nodes
            .get_mut(index)
            .expect("rbtree node index is valid")
    }

    fn find_node(&self, key: K) -> Option<PoolIndex> {
        let mut cursor = self.root;
        while let Some(index) = cursor {
            let node = self.node(index);
            match key.cmp(&node.key) {
                core::cmp::Ordering::Less => cursor = node.left,
                core::cmp::Ordering::Greater => cursor = node.right,
                core::cmp::Ordering::Equal => return Some(index),
            }
        }
        None
    }

    fn minimum_index(&self, mut index: PoolIndex) -> PoolIndex {
        while let Some(left) = self.node(index).left {
            index = left;
        }
        index
    }

    fn maximum_index(&self, mut index: PoolIndex) -> PoolIndex {
        while let Some(right) = self.node(index).right {
            index = right;
        }
        index
    }

    fn predecessor_index(&self, key: K) -> Option<PoolIndex> {
        let mut cursor = self.root;
        let mut candidate = None;
        while let Some(index) = cursor {
            let node = self.node(index);
            if key <= node.key {
                cursor = node.left;
            } else {
                candidate = Some(index);
                cursor = node.right;
            }
        }
        candidate
    }

    fn successor_index(&self, key: K) -> Option<PoolIndex> {
        let mut cursor = self.root;
        let mut candidate = None;
        while let Some(index) = cursor {
            let node = self.node(index);
            if key < node.key {
                candidate = Some(index);
                cursor = node.left;
            } else {
                cursor = node.right;
            }
        }
        candidate
    }

    fn rotate_left(&mut self, x: PoolIndex) {
        let y = self
            .node(x)
            .right
            .expect("left rotation requires right child");
        let y_left = self.node(y).left;
        self.node_mut(x).right = y_left;
        if let Some(y_left_index) = y_left {
            self.node_mut(y_left_index).parent = Some(x);
        }
        let x_parent = self.node(x).parent;
        self.node_mut(y).parent = x_parent;
        if let Some(parent) = x_parent {
            if self.node(parent).left == Some(x) {
                self.node_mut(parent).left = Some(y);
            } else {
                self.node_mut(parent).right = Some(y);
            }
        } else {
            self.root = Some(y);
        }
        self.node_mut(y).left = Some(x);
        self.node_mut(x).parent = Some(y);
    }

    fn rotate_right(&mut self, x: PoolIndex) {
        let y = self
            .node(x)
            .left
            .expect("right rotation requires left child");
        let y_right = self.node(y).right;
        self.node_mut(x).left = y_right;
        if let Some(y_right_index) = y_right {
            self.node_mut(y_right_index).parent = Some(x);
        }
        let x_parent = self.node(x).parent;
        self.node_mut(y).parent = x_parent;
        if let Some(parent) = x_parent {
            if self.node(parent).right == Some(x) {
                self.node_mut(parent).right = Some(y);
            } else {
                self.node_mut(parent).left = Some(y);
            }
        } else {
            self.root = Some(y);
        }
        self.node_mut(y).right = Some(x);
        self.node_mut(x).parent = Some(y);
    }

    fn insert_fixup(&mut self, mut z: PoolIndex) {
        while let Some(parent) = self.node(z).parent {
            if self.node(parent).color == Color::Black {
                break;
            }
            let grandparent = self
                .node(parent)
                .parent
                .expect("red parent must have grandparent");
            if self.node(grandparent).left == Some(parent) {
                let uncle = self.node(grandparent).right;
                if self.color_of(uncle) == Color::Red {
                    self.node_mut(parent).color = Color::Black;
                    if let Some(uncle_index) = uncle {
                        self.node_mut(uncle_index).color = Color::Black;
                    }
                    self.node_mut(grandparent).color = Color::Red;
                    z = grandparent;
                } else {
                    if self.node(parent).right == Some(z) {
                        z = parent;
                        self.rotate_left(z);
                    }
                    let parent = self.node(z).parent.expect("parent exists after rotate");
                    let grandparent = self.node(parent).parent.expect("grandparent exists");
                    self.node_mut(parent).color = Color::Black;
                    self.node_mut(grandparent).color = Color::Red;
                    self.rotate_right(grandparent);
                }
            } else {
                let uncle = self.node(grandparent).left;
                if self.color_of(uncle) == Color::Red {
                    self.node_mut(parent).color = Color::Black;
                    if let Some(uncle_index) = uncle {
                        self.node_mut(uncle_index).color = Color::Black;
                    }
                    self.node_mut(grandparent).color = Color::Red;
                    z = grandparent;
                } else {
                    if self.node(parent).left == Some(z) {
                        z = parent;
                        self.rotate_right(z);
                    }
                    let parent = self.node(z).parent.expect("parent exists after rotate");
                    let grandparent = self.node(parent).parent.expect("grandparent exists");
                    self.node_mut(parent).color = Color::Black;
                    self.node_mut(grandparent).color = Color::Red;
                    self.rotate_left(grandparent);
                }
            }
        }
        if let Some(root) = self.root {
            self.node_mut(root).color = Color::Black;
        }
    }

    fn transplant(&mut self, u: PoolIndex, v: Option<PoolIndex>) {
        let parent = self.node(u).parent;
        if let Some(parent_index) = parent {
            if self.node(parent_index).left == Some(u) {
                self.node_mut(parent_index).left = v;
            } else {
                self.node_mut(parent_index).right = v;
            }
        } else {
            self.root = v;
        }
        if let Some(v_index) = v {
            self.node_mut(v_index).parent = parent;
        }
    }

    fn delete_fixup(&mut self, mut x: Option<PoolIndex>, mut x_parent: Option<PoolIndex>) {
        while x != self.root && self.color_of(x) == Color::Black {
            let Some(parent) = x_parent else {
                break;
            };
            if self.node(parent).left == x {
                let mut w = self.node(parent).right;
                if self.color_of(w) == Color::Red {
                    let w_index = w.expect("red sibling exists");
                    self.node_mut(w_index).color = Color::Black;
                    self.node_mut(parent).color = Color::Red;
                    self.rotate_left(parent);
                    w = self.node(parent).right;
                }
                let w_index = match w {
                    Some(index) => index,
                    None => {
                        x = Some(parent);
                        x_parent = self.node(parent).parent;
                        continue;
                    }
                };
                if self.color_of(self.node(w_index).left) == Color::Black
                    && self.color_of(self.node(w_index).right) == Color::Black
                {
                    self.node_mut(w_index).color = Color::Red;
                    x = Some(parent);
                    x_parent = self.node(parent).parent;
                } else {
                    if self.color_of(self.node(w_index).right) == Color::Black {
                        if let Some(left) = self.node(w_index).left {
                            self.node_mut(left).color = Color::Black;
                        }
                        self.node_mut(w_index).color = Color::Red;
                        self.rotate_right(w_index);
                        w = self.node(parent).right;
                    }
                    let w_index = w.expect("sibling exists after rotation");
                    let parent_color = self.node(parent).color;
                    self.node_mut(w_index).color = parent_color;
                    self.node_mut(parent).color = Color::Black;
                    if let Some(right) = self.node(w_index).right {
                        self.node_mut(right).color = Color::Black;
                    }
                    self.rotate_left(parent);
                    x = self.root;
                    x_parent = None;
                }
            } else {
                let mut w = self.node(parent).left;
                if self.color_of(w) == Color::Red {
                    let w_index = w.expect("red sibling exists");
                    self.node_mut(w_index).color = Color::Black;
                    self.node_mut(parent).color = Color::Red;
                    self.rotate_right(parent);
                    w = self.node(parent).left;
                }
                let w_index = match w {
                    Some(index) => index,
                    None => {
                        x = Some(parent);
                        x_parent = self.node(parent).parent;
                        continue;
                    }
                };
                if self.color_of(self.node(w_index).right) == Color::Black
                    && self.color_of(self.node(w_index).left) == Color::Black
                {
                    self.node_mut(w_index).color = Color::Red;
                    x = Some(parent);
                    x_parent = self.node(parent).parent;
                } else {
                    if self.color_of(self.node(w_index).left) == Color::Black {
                        if let Some(right) = self.node(w_index).right {
                            self.node_mut(right).color = Color::Black;
                        }
                        self.node_mut(w_index).color = Color::Red;
                        self.rotate_left(w_index);
                        w = self.node(parent).left;
                    }
                    let w_index = w.expect("sibling exists after rotation");
                    let parent_color = self.node(parent).color;
                    self.node_mut(w_index).color = parent_color;
                    self.node_mut(parent).color = Color::Black;
                    if let Some(left) = self.node(w_index).left {
                        self.node_mut(left).color = Color::Black;
                    }
                    self.rotate_right(parent);
                    x = self.root;
                    x_parent = None;
                }
            }
        }
        if let Some(x_index) = x {
            self.node_mut(x_index).color = Color::Black;
        }
    }

    #[inline]
    fn color_of(&self, index: Option<PoolIndex>) -> Color {
        index.map_or(Color::Black, |node| self.node(node).color)
    }
}

impl<K, V> Default for RbTree<K, V>
where
    K: Ord + Copy,
{
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

pub struct Iter<'a, K, V>
where
    K: Ord + Copy,
{
    tree: &'a RbTree<K, V>,
    stack: Vec<PoolIndex>,
}

impl<'a, K, V> Iterator for Iter<'a, K, V>
where
    K: Ord + Copy,
{
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        let index = self.stack.pop()?;
        let node = self.tree.node(index);
        let mut cursor = node.right;
        while let Some(right) = cursor {
            self.stack.push(right);
            cursor = self.tree.node(right).left;
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
        tree.insert(20u32, "b");
        tree.insert(10u32, "a");
        tree.insert(30u32, "c");

        let items: std::vec::Vec<_> = tree.iter().map(|(k, v)| (*k, *v)).collect();
        assert_eq!(items, vec![(10, "a"), (20, "b"), (30, "c")]);
    }

    #[test]
    fn rbtree_predecessor_and_successor_follow_in_order_neighbors() {
        let mut tree = RbTree::new();
        tree.insert(10u32, "a");
        tree.insert(20u32, "b");
        tree.insert(30u32, "c");

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
        tree.insert(100u32, "a");
        tree.insert(200u32, "b");
        tree.insert(300u32, "c");

        assert_eq!(tree.predecessor(&250).map(|(k, _)| *k), Some(200));
        assert_eq!(tree.successor(&250).map(|(k, _)| *k), Some(300));
    }

    #[test]
    fn rbtree_remove_preserves_remaining_order() {
        let mut tree = RbTree::new();
        tree.insert(10u32, "a");
        tree.insert(20u32, "b");
        tree.insert(30u32, "c");

        assert_eq!(tree.remove(&20), Some("b"));
        let items: std::vec::Vec<_> = tree.iter().map(|(k, v)| (*k, *v)).collect();
        assert_eq!(items, vec![(10, "a"), (30, "c")]);
    }

    #[test]
    fn rbtree_first_last_and_get_mut_track_extreme_and_in_place_updates() {
        let mut tree = RbTree::new();
        tree.insert(20u32, 2u32);
        tree.insert(10u32, 1u32);
        tree.insert(30u32, 3u32);

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
        tree.insert(10u32, "a");
        tree.insert(20u32, "b");
        tree.insert(30u32, "c");
        tree.prefetch_first();
        tree.prefetch_node(&20);
        tree.prefetch_node(&999);
        assert_eq!(tree.len(), 3);
        assert_eq!(tree.get(&20), Some(&"b"));
    }
}
