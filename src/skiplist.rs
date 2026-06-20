//! 跳表：LSM-Tree 的 MemTable 底层结构。
//!
//! 用 arena 索引实现：所有节点存于一个 `Vec`，节点间的指针是 `usize` 索引。
//! 这样避开图结构在 Rust 所权模型下的困难，且零 `unsafe`。

use rand::Rng;

/// 节点高度上限。足够覆盖百万级数据量（期望高度 ≈ log_(1/p)(n)）。
const MAX_HEIGHT: usize = 12;

/// 分支因子：单层提升概率 = 1/BRANCHING。值越大结构越扁平。
const BRANCHING: usize = 4;

/// next 指针用此值表示"该层无后继"（NIL）。
const NIL: usize = usize::MAX;

/// 跳表节点。`next` 数组长度 = 节点高度，`next[0]` 是底层后继。
/// 同一个节点被所有它出现的层级共用。
struct Node<K, V> {
    key: K,
    // get() 在 1.3 实现，届时移除此 allow。
    #[allow(dead_code)]
    value: V,
    next: Vec<usize>,
}

impl<K, V> Node<K, V> {
    fn new(key: K, value: V, height: usize) -> Self {
        let mut next = Vec::with_capacity(height);
        next.resize(height, NIL);
        Node { key, value, next }
    }
}

/// 跳表。所有节点存在 `nodes` arena 里，用索引互相引用。
pub struct SkipList<K, V> {
    nodes: Vec<Node<K, V>>,
    /// 当前实际使用的最大层级。head 拥有全部 MAX_HEIGHT 层，此值随插入增长。
    max_height: usize,
}

impl<K, V> SkipList<K, V> {
    /// head 固定放在 arena 的 0 号位。
    const HEAD: usize = 0;

    pub fn max_height(&self) -> usize {
        self.max_height
    }
}

impl<K: Ord, V> SkipList<K, V> {
    /// 从 head 最高层开始，逐层下降到第 0 层，记录每一层中
    /// "key 小于 target 的最后一个节点"作为插入前驱。
    /// 返回 prev[i] 即第 i 层新节点应插入位置的前驱索引。
    fn find_prev(&self, key: &K) -> Vec<usize> {
        let mut prev = vec![0; self.max_height];
        let mut cur: usize = Self::HEAD;
        // 从最高层往下找：高层负责"跳"，底层负责"精确定位"。
        for level in (0..self.max_height).rev() {
            while self.nodes[cur].next[level] != NIL
                && self.nodes[self.nodes[cur].next[level]].key < *key
            {
                cur = self.nodes[cur].next[level];
            }
            prev[level] = cur;
        }
        prev
    }

    /// 几何分布抽层高：每层有 1/BRANCHING 概率再升一层。
    /// 期望高度 1/(1 - 1/BRANCHING)，平衡效果等价于红黑树但无需旋转。
    fn random_height(rng: &mut impl Rng) -> usize {
        let mut height = 1;
        while height < MAX_HEIGHT && rng.random_range(0..BRANCHING) == 0 {
            height += 1;
        }
        height
    }

    /// 插入一个节点。允许重复 key（LSM 需要同一 user_key 的多个版本）。
    pub fn insert(&mut self, key: K, value: V) {
        let mut rng = rand::rng();
        let height = Self::random_height(&mut rng);
        let mut prev = self.find_prev(&key);

        // 新节点比当前最高层还高：高层目前只有 head，那些层的前驱补成 head。
        if height > self.max_height {
            for _ in self.max_height..height {
                prev.push(Self::HEAD);
            }
            self.max_height = height;
        }

        let new_index = self.nodes.len();
        let mut new_node = Node::new(key, value, height);
        // 经典链表插入，在每一层缝合：new.next = prev.next; prev.next = new。
        for level in 0..height {
            new_node.next[level] = self.nodes[prev[level]].next[level];
            self.nodes[prev[level]].next[level] = new_index;
        }
        self.nodes.push(new_node);
    }
}

impl<K: Ord + Default, V: Default> SkipList<K, V> {
    /// 创建空跳表。head 作为哑节点占据索引 0，高度固定为 MAX_HEIGHT。
    /// head 的 key/value 用 Default 占位，永不参与比较。
    pub fn new() -> Self {
        let head = Node::new(K::default(), V::default(), MAX_HEIGHT);
        SkipList {
            nodes: vec![head],
            max_height: 1,
        }
    }
}

impl<K: Ord + Default, V: Default> Default for SkipList<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_keeps_bottom_level_sorted() {
        let mut sl: SkipList<i32, i32> = SkipList::new();
        // 乱序插入，验证底层链表最终有序。
        for k in [50, 10, 40, 20, 30, 60, 5, 15] {
            sl.insert(k, k * 10);
        }
        // 沿 next[0] 遍历底层（含全部数据），应严格升序。
        let mut cur = SkipList::<i32, i32>::HEAD;
        let mut prev = i32::MIN;
        let mut count = 0;
        while sl.nodes[cur].next[0] != NIL {
            cur = sl.nodes[cur].next[0];
            assert!(sl.nodes[cur].key > prev, "bottom level not sorted");
            prev = sl.nodes[cur].key;
            count += 1;
        }
        assert_eq!(count, 8);
    }

    #[test]
    fn large_random_insert_stays_sorted() {
        let mut sl: SkipList<i32, i32> = SkipList::new();
        let mut rng = rand::rng();
        let keys: Vec<i32> = (0..1000).map(|_| rng.random_range(0..100_000)).collect();
        for &k in &keys {
            sl.insert(k, k);
        }
        // 底层遍历必须全局有序。
        let mut cur = SkipList::<i32, i32>::HEAD;
        let mut prev = i32::MIN;
        while sl.nodes[cur].next[0] != NIL {
            cur = sl.nodes[cur].next[0];
            assert!(sl.nodes[cur].key >= prev);
            prev = sl.nodes[cur].key;
        }
    }

    #[test]
    fn allows_duplicate_keys() {
        let mut sl: SkipList<i32, i32> = SkipList::new();
        // 同一个 key 允许多个版本（LSM 多版本需求）。
        sl.insert(5, 50);
        sl.insert(5, 51);
        sl.insert(5, 52);
        let mut cur = SkipList::<i32, i32>::HEAD;
        let mut count = 0;
        while sl.nodes[cur].next[0] != NIL {
            cur = sl.nodes[cur].next[0];
            if sl.nodes[cur].key == 5 {
                count += 1;
            }
        }
        assert_eq!(count, 3);
    }
}
