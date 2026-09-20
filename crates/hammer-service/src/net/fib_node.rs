use std::mem::{offset_of, size_of};

use hammer_infra::pool::Pool;

const INVALID: u32 = u32::MAX;
const WALK_TYPE: FibNodeType = FibNodeType(u8::MAX);

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FibNodeType(u8);

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FibNodePtr {
    pub node_type: FibNodeType,
    pub index: u32,
}

impl FibNodePtr {
    pub const fn new(node_type: FibNodeType, index: u32) -> Self {
        Self { node_type, index }
    }
}

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FibNodeList(u32);

impl FibNodeList {
    pub const NONE: Self = Self(INVALID);
}

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FibNodeSibling(u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FibWalkPriority {
    High,
    Low,
}

impl FibWalkPriority {
    const fn index(self) -> usize {
        match self {
            Self::High => 0,
            Self::Low => 1,
        }
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct FibNode {
    node_type: FibNodeType,
    owner_data: u16,
    children: FibNodeList,
    lock_count: u32,
}

impl FibNode {
    pub const fn new(node_type: FibNodeType) -> Self {
        Self {
            node_type,
            owner_data: 0,
            children: FibNodeList::NONE,
            lock_count: 0,
        }
    }

    pub fn lock(&mut self) {
        self.lock_count = self
            .lock_count
            .checked_add(1)
            .expect("FIB node lock overflow");
    }

    pub fn unlock(&mut self) -> bool {
        self.lock_count = self
            .lock_count
            .checked_sub(1)
            .expect("FIB node lock underflow");
        self.lock_count == 0
    }

    pub const fn children(&self) -> FibNodeList {
        self.children
    }

    pub fn set_children(&mut self, children: FibNodeList) {
        self.children = children;
    }

    pub fn assert_detached(&self) {
        assert_eq!(
            self.children,
            FibNodeList::NONE,
            "FIB node still has children"
        );
        assert_eq!(self.lock_count, 0, "FIB node still has references");
    }
}

const _: () = assert!(size_of::<FibNode>() == 12);
const _: () = assert!(offset_of!(FibNode, children) == 4);
const _: () = assert!(offset_of!(FibNode, lock_count) == 8);
const _: () = assert!(size_of::<FibNodePtr>() == 8);
const _: () = assert!(offset_of!(FibNodePtr, index) == 4);

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct FibWalkReason: u16 {
        const EVALUATE = 1 << 0;
        const ADJ_UPDATE = 1 << 1;
        const ADJ_DOWN = 1 << 2;
        const ADJ_MTU = 1 << 3;
        const INTERFACE_UP = 1 << 4;
        const INTERFACE_DOWN = 1 << 5;
        const INTERFACE_DELETE = 1 << 6;
        const INTERFACE_BIND = 1 << 7;
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct FibWalkFlags: u8 {
        const FORCE_SYNC = 1 << 0;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FibNodeBackWalkContext {
    pub reason: FibWalkReason,
    pub flags: FibWalkFlags,
    pub depth: u8,
    pub old_table: u32,
    pub new_table: u32,
}

impl FibNodeBackWalkContext {
    pub fn new(reason: FibWalkReason) -> Self {
        Self {
            reason,
            flags: FibWalkFlags::empty(),
            depth: 0,
            old_table: INVALID,
            new_table: INVALID,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FibNodeBackWalkResult {
    Continue,
    Merge,
}

#[derive(Clone, Copy)]
pub struct FibNodeOperations {
    pub lock: fn(u32),
    pub unlock: fn(u32) -> bool,
    pub children: fn(u32) -> FibNodeList,
    pub set_children: fn(u32, FibNodeList),
    pub last_lock: fn(u32),
    pub back_walk: Option<
        fn(
            &mut FibNodeMain,
            &mut hammer_runtime::DataPlaneMain,
            u32,
            &mut FibNodeBackWalkContext,
        ) -> FibNodeBackWalkResult,
    >,
    pub memory: Option<fn() -> (usize, usize, usize)>,
}

impl FibNodeOperations {
    pub const fn new(
        lock: fn(u32),
        unlock: fn(u32) -> bool,
        children: fn(u32) -> FibNodeList,
        set_children: fn(u32, FibNodeList),
        last_lock: fn(u32),
    ) -> Self {
        Self {
            lock,
            unlock,
            children,
            set_children,
            last_lock,
            back_walk: None,
            memory: None,
        }
    }

    pub const fn with_back_walk(
        mut self,
        back_walk: fn(
            &mut FibNodeMain,
            &mut hammer_runtime::DataPlaneMain,
            u32,
            &mut FibNodeBackWalkContext,
        ) -> FibNodeBackWalkResult,
    ) -> Self {
        self.back_walk = Some(back_walk);
        self
    }

    pub const fn with_memory(mut self, memory: fn() -> (usize, usize, usize)) -> Self {
        self.memory = Some(memory);
        self
    }
}

struct FibNodeListHead {
    first: u32,
    last: u32,
    count: u32,
}

struct FibNodeListElement {
    list: FibNodeList,
    child: FibNodePtr,
    previous: u32,
    next: u32,
}

struct FibWalk {
    node: FibNode,
    parent: FibNodePtr,
    parent_sibling: FibNodeSibling,
    queue_sibling: Option<FibNodeSibling>,
    contexts: Vec<FibNodeBackWalkContext>,
    executing: bool,
}

pub struct FibNodeMain {
    types: Vec<(&'static str, FibNodeOperations)>,
    lists: Pool<FibNodeListHead>,
    elements: Pool<FibNodeListElement>,
    walks: Pool<FibWalk>,
    queues: [FibNodeList; 2],
}

impl Default for FibNodeMain {
    fn default() -> Self {
        Self {
            types: Vec::new(),
            lists: Pool::new(),
            elements: Pool::new(),
            walks: Pool::new(),
            queues: [FibNodeList::NONE; 2],
        }
    }
}

impl FibNodeMain {
    pub fn register_type(
        &mut self,
        name: &'static str,
        operations: FibNodeOperations,
    ) -> FibNodeType {
        hammer_runtime::ensure_main_thread_with_barrier()
            .expect("FIB node registration requires publication ownership");
        assert!(
            self.types.iter().all(|(registered, _)| *registered != name),
            "FIB node type registered twice: {name}"
        );
        let index = u8::try_from(self.types.len()).expect("FIB node type space exhausted");
        assert_ne!(index, WALK_TYPE.0, "FIB node type space exhausted");
        self.types.push((name, operations));
        FibNodeType(index)
    }

    fn operations(&self, node: FibNodePtr) -> FibNodeOperations {
        self.types
            .get(node.node_type.0 as usize)
            .expect("FIB node type must be registered")
            .1
    }

    pub fn lock(&mut self, node: FibNodePtr) {
        if node.node_type == WALK_TYPE {
            self.walks
                .get_mut(node.index)
                .expect("live FIB walk")
                .node
                .lock();
        } else {
            (self.operations(node).lock)(node.index);
        }
    }

    pub fn unlock(&mut self, node: FibNodePtr) {
        if node.node_type == WALK_TYPE {
            self.walks
                .get_mut(node.index)
                .expect("live FIB walk")
                .node
                .unlock();
            return;
        }
        let operations = self.operations(node);
        if (operations.unlock)(node.index) {
            (operations.last_lock)(node.index);
        }
    }

    pub fn child_add(&mut self, parent: FibNodePtr, child: FibNodePtr) -> FibNodeSibling {
        hammer_runtime::ensure_main_thread_with_barrier()
            .expect("FIB graph mutation requires publication ownership");
        assert!(
            child.node_type == WALK_TYPE || self.operations(child).back_walk.is_some(),
            "FIB child must implement back-walk"
        );
        let operations = self.operations(parent);
        let mut list = (operations.children)(parent.index);
        if list == FibNodeList::NONE {
            list = self.new_list();
            (operations.set_children)(parent.index, list);
        }
        let sibling = self.list_insert(list, child, child.node_type == WALK_TYPE);
        (operations.lock)(parent.index);
        sibling
    }

    pub fn child_remove(&mut self, parent: FibNodePtr, sibling: FibNodeSibling) {
        hammer_runtime::ensure_main_thread_with_barrier()
            .expect("FIB graph mutation requires publication ownership");
        let operations = self.operations(parent);
        let list = (operations.children)(parent.index);
        self.list_remove(list, sibling);
        if self
            .lists
            .get(list.0)
            .expect("FIB child list must exist")
            .count
            == 0
        {
            self.lists
                .remove(list.0)
                .expect("empty FIB child list must exist");
            (operations.set_children)(parent.index, FibNodeList::NONE);
        }
        self.unlock(parent);
    }

    pub fn child_count(&self, parent: FibNodePtr) -> u32 {
        let list = (self.operations(parent).children)(parent.index);
        if list == FibNodeList::NONE {
            return 0;
        }
        self.lists
            .get(list.0)
            .expect("FIB child list must exist")
            .count
    }

    fn new_list(&mut self) -> FibNodeList {
        FibNodeList(self.lists.insert(FibNodeListHead {
            first: INVALID,
            last: INVALID,
            count: 0,
        }))
    }

    fn list_insert(&mut self, list: FibNodeList, child: FibNodePtr, front: bool) -> FibNodeSibling {
        let head = self
            .lists
            .get_mut(list.0)
            .expect("FIB child list must exist");
        let sibling = self.elements.insert(FibNodeListElement {
            list,
            child,
            previous: if front { INVALID } else { head.last },
            next: if front { head.first } else { INVALID },
        });
        let adjacent = if front { head.first } else { head.last };
        if adjacent == INVALID {
            head.first = sibling;
            head.last = sibling;
        } else if front {
            self.elements
                .get_mut(adjacent)
                .expect("FIB list head must exist")
                .previous = sibling;
            head.first = sibling;
        } else {
            self.elements
                .get_mut(adjacent)
                .expect("FIB list tail must exist")
                .next = sibling;
            head.last = sibling;
        }
        head.count += 1;
        FibNodeSibling(sibling)
    }

    fn list_remove(&mut self, list: FibNodeList, sibling: FibNodeSibling) {
        let element = self
            .elements
            .get(sibling.0)
            .expect("FIB sibling must exist");
        assert_eq!(element.list, list, "FIB sibling belongs to another parent");
        let (previous, next) = (element.previous, element.next);
        let head = self
            .lists
            .get_mut(list.0)
            .expect("FIB child list must exist");
        if previous == INVALID {
            head.first = next;
        } else {
            self.elements
                .get_mut(previous)
                .expect("FIB previous sibling must exist")
                .next = next;
        }
        if next == INVALID {
            head.last = previous;
        } else {
            self.elements
                .get_mut(next)
                .expect("FIB next sibling must exist")
                .previous = previous;
        }
        head.count -= 1;
        self.elements
            .remove(sibling.0)
            .expect("FIB sibling must exist");
    }

    fn list_advance(&mut self, sibling: FibNodeSibling, past: u32) {
        let (list, previous, next) = {
            let element = self
                .elements
                .get(sibling.0)
                .expect("FIB walk sibling must exist");
            (element.list, element.previous, element.next)
        };
        let past_next = self
            .elements
            .get(past)
            .expect("visited FIB child must exist")
            .next;
        assert_eq!(
            self.elements
                .get(past)
                .expect("visited FIB child must exist")
                .list,
            list
        );
        assert_eq!(next, past, "FIB walk advances over its next child");
        let head = self
            .lists
            .get_mut(list.0)
            .expect("FIB child list must exist");
        if previous == INVALID {
            head.first = next;
        } else {
            self.elements
                .get_mut(previous)
                .expect("FIB previous sibling must exist")
                .next = next;
        }
        if next == INVALID {
            head.last = previous;
        } else {
            self.elements
                .get_mut(next)
                .expect("FIB next sibling must exist")
                .previous = previous;
        }
        self.elements
            .get_mut(past)
            .expect("visited FIB child must exist")
            .next = sibling.0;
        if past_next == INVALID {
            head.last = sibling.0;
        } else {
            self.elements
                .get_mut(past_next)
                .expect("FIB next sibling must exist")
                .previous = sibling.0;
        }
        let walk = self
            .elements
            .get_mut(sibling.0)
            .expect("FIB walk sibling must exist");
        walk.previous = past;
        walk.next = past_next;
    }

    fn start_walk(&mut self, parent: FibNodePtr, context: FibNodeBackWalkContext) -> Option<u32> {
        if self.child_count(parent) == 0 {
            return None;
        }
        let index = self.walks.insert(FibWalk {
            node: FibNode::new(WALK_TYPE),
            parent,
            parent_sibling: FibNodeSibling(INVALID),
            queue_sibling: None,
            contexts: vec![context],
            executing: false,
        });
        let sibling = self.child_add(parent, FibNodePtr::new(WALK_TYPE, index));
        self.walks
            .get_mut(index)
            .expect("new FIB walk exists")
            .parent_sibling = sibling;
        Some(index)
    }

    fn finish_walk(&mut self, index: u32) {
        let walk = self.walks.get(index).expect("FIB walk must be live");
        let (parent, parent_sibling, queue_sibling) =
            (walk.parent, walk.parent_sibling, walk.queue_sibling);
        if let Some(queue_sibling) = queue_sibling {
            let queue = self
                .elements
                .get(queue_sibling.0)
                .expect("queued FIB walk exists")
                .list;
            self.list_remove(queue, queue_sibling);
        }
        self.child_remove(parent, parent_sibling);
        self.walks
            .remove(index)
            .expect("completed FIB walk exists")
            .node
            .assert_detached();
    }

    fn advance_walk(
        &mut self,
        main: &mut hammer_runtime::DataPlaneMain,
        index: u32,
    ) -> Option<u32> {
        let sibling = self
            .walks
            .get(index)
            .expect("FIB walk exists")
            .parent_sibling;
        let next = self
            .elements
            .get(sibling.0)
            .expect("FIB walk is attached")
            .next;
        if next == INVALID {
            self.finish_walk(index);
            return None;
        }
        let child = self.elements.get(next).expect("FIB child exists").child;
        let mut position = 0;
        loop {
            let walk = self.walks.get(index).expect("FIB walk exists");
            if position == walk.contexts.len() {
                break;
            }
            let mut context = walk.contexts[position];
            let outcome = if child.node_type == WALK_TYPE {
                let target = self
                    .walks
                    .get_mut(child.index)
                    .expect("preceding FIB walk exists");
                if let Some(last) = target
                    .contexts
                    .last_mut()
                    .filter(|last| last.reason == context.reason)
                {
                    last.depth = last.depth.max(context.depth);
                } else {
                    target.contexts.push(context);
                }
                FibNodeBackWalkResult::Merge
            } else {
                let callback = self
                    .operations(child)
                    .back_walk
                    .expect("FIB child implements back-walk");
                callback(self, main, child.index, &mut context)
            };
            if outcome == FibNodeBackWalkResult::Merge {
                self.finish_walk(index);
                return Some(child.index);
            }
            position += 1;
        }
        if self.elements.get(next).is_some() {
            self.list_advance(sibling, next);
        }
        None
    }

    fn run_walk(&mut self, main: &mut hammer_runtime::DataPlaneMain, mut index: u32) {
        loop {
            if self.walks.get(index).expect("FIB walk exists").executing {
                return;
            }
            self.walks
                .get_mut(index)
                .expect("FIB walk exists")
                .executing = true;
            while self.walks.get(index).is_some() {
                if let Some(merged) = self.advance_walk(main, index) {
                    index = merged;
                    break;
                }
            }
            if self.walks.get(index).is_none() {
                return;
            }
            if self
                .walks
                .get(index)
                .expect("merged FIB walk exists")
                .executing
            {
                return;
            }
        }
    }

    pub fn walk_sync(
        &mut self,
        main: &mut hammer_runtime::DataPlaneMain,
        parent: FibNodePtr,
        context: &mut FibNodeBackWalkContext,
    ) {
        hammer_runtime::ensure_main_thread_with_barrier()
            .expect("FIB walk requires publication ownership");
        assert!(context.depth < 32, "FIB graph back-walk depth exceeded 32");
        let mut next = *context;
        next.depth += 1;
        if let Some(index) = self.start_walk(parent, next) {
            self.run_walk(main, index);
        }
    }

    pub fn walk_async(
        &mut self,
        main: &mut hammer_runtime::DataPlaneMain,
        parent: FibNodePtr,
        priority: FibWalkPriority,
        context: FibNodeBackWalkContext,
    ) {
        hammer_runtime::ensure_main_thread_with_barrier()
            .expect("FIB walk requires publication ownership");
        assert!(context.depth < 32, "FIB graph back-walk depth exceeded 32");
        if context.flags.contains(FibWalkFlags::FORCE_SYNC) {
            let mut context = context;
            self.walk_sync(main, parent, &mut context);
            return;
        }
        let mut next = context;
        next.depth += 1;
        if let Some(index) = self.start_walk(parent, next) {
            if self.queues[priority.index()] == FibNodeList::NONE {
                self.queues[priority.index()] = self.new_list();
            }
            let queue = self.queues[priority.index()];
            let sibling = self.list_insert(queue, FibNodePtr::new(WALK_TYPE, index), false);
            self.walks
                .get_mut(index)
                .expect("new FIB walk exists")
                .queue_sibling = Some(sibling);
        }
    }

    pub fn run_queued_walks(
        &mut self,
        main: &mut hammer_runtime::DataPlaneMain,
        budget: usize,
    ) -> usize {
        hammer_runtime::ensure_main_thread_with_barrier()
            .expect("FIB walk requires publication ownership");
        let mut processed = 0;
        for priority in [FibWalkPriority::High, FibWalkPriority::Low] {
            while processed < budget {
                let queue = self.queues[priority.index()];
                if queue == FibNodeList::NONE {
                    break;
                }
                let head = self
                    .lists
                    .get(queue.0)
                    .expect("FIB walk queue exists")
                    .first;
                if head == INVALID {
                    break;
                }
                let index = self
                    .elements
                    .get(head)
                    .expect("queued FIB walk exists")
                    .child
                    .index;
                self.walks
                    .get_mut(index)
                    .expect("queued FIB walk exists")
                    .executing = true;
                let merged = self.advance_walk(main, index);
                if let Some(walk) = self.walks.get_mut(index) {
                    walk.executing = false;
                }
                if let Some(merged) = merged {
                    if self.walks.get(merged).is_some()
                        && !self
                            .walks
                            .get(merged)
                            .expect("merged walk exists")
                            .executing
                    {
                        self.run_walk(main, merged);
                    }
                }
                processed += 1;
            }
        }
        processed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn walk_sibling_advances_past_visited_child() {
        let mut nodes = FibNodeMain::default();
        let list = nodes.new_list();
        let entry_type = FibNodeType(1);
        let first = nodes.list_insert(list, FibNodePtr::new(entry_type, 10), false);
        let second = nodes.list_insert(list, FibNodePtr::new(entry_type, 11), false);
        let older = nodes.list_insert(list, FibNodePtr::new(WALK_TYPE, 20), true);

        nodes.list_advance(older, first.0);
        assert_eq!(nodes.lists.get(list.0).unwrap().first, first.0);
        assert_eq!(nodes.elements.get(first.0).unwrap().next, older.0);
        assert_eq!(nodes.elements.get(older.0).unwrap().next, second.0);

        let newer = nodes.list_insert(list, FibNodePtr::new(WALK_TYPE, 21), true);
        nodes.list_advance(newer, first.0);
        assert_eq!(nodes.elements.get(newer.0).unwrap().next, older.0);
        nodes.list_remove(list, older);
        assert_eq!(nodes.elements.get(newer.0).unwrap().next, second.0);
    }
}
