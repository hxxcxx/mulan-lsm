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

    /// 从底层链表头部开始遍历。底层 next[0] 是含全部数据的完整有序链表。
    pub fn iter(&self) -> Iter<'_, K, V> {
        Iter {
            list: self,
            cur: self.nodes[Self::HEAD].next[0],
        }
    }
}

/// 跳表迭代器：沿底层 next[0] 顺链走，天然有序。
/// 是 LSM range scan 的基础。
pub struct Iter<'a, K, V> {
    list: &'a SkipList<K, V>,
    cur: usize,
}

impl<'a, K, V> Iterator for Iter<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        if self.cur == NIL {
            return None;
        }
        let node = &self.list.nodes[self.cur];
        let item = (&node.key, &node.value);
        self.cur = node.next[0];
        Some(item)
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

    /// 精确查找 key，返回对应 value 的引用。未找到返回 None。
    ///
    /// 下降逻辑与 find_prev 一致：从最高层往下降，每层向右走到
    /// "后继 key >= target"处停。最后看底层后继 key 是否恰好等于 target。
    pub fn get(&self, key: &K) -> Option<&V> {
        let mut cur: usize = Self::HEAD;
        for level in (0..self.max_height).rev() {
            while self.nodes[cur].next[level] != NIL
                && self.nodes[self.nodes[cur].next[level]].key < *key
            {
                cur = self.nodes[cur].next[level];
            }
        }
        // cur 是底层"key < target 的最后一个节点"，看它的后继是否命中。
        let next = self.nodes[cur].next[0];
        if next != NIL && self.nodes[next].key == *key {
            Some(&self.nodes[next].value)
        } else {
            None
        }
    }

    /// 返回第一个 key >= target 的条目。范围查询的基础。
    ///
    /// 下降逻辑与 get 一致：停在前驱节点后，取底层后继即为第一个 >= target。
    /// MemTable 的 get 用此方法 + 哨兵 key 定位 user_key 的最新版本。
    pub fn lower_bound(&self, key: &K) -> Option<(&K, &V)> {
        let mut cur: usize = Self::HEAD;
        for level in (0..self.max_height).rev() {
            while self.nodes[cur].next[level] != NIL
                && self.nodes[self.nodes[cur].next[level]].key < *key
            {
                cur = self.nodes[cur].next[level];
            }
        }
        let next = self.nodes[cur].next[0];
        if next != NIL {
            Some((&self.nodes[next].key, &self.nodes[next].value))
        } else {
            None
        }
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
        // 用 enumerate 拿到层号 level（new_node.next 的下标天然就是层号），
        // 避免 clippy 的 needless_range_loop。循环体先读 prev 的后继再回写，
        // 读写在不同的 arena 索引表达式里，不构成借用冲突。
        for (level, new_next) in new_node.next.iter_mut().enumerate() {
            let prev_node = &mut self.nodes[prev[level]];
            *new_next = prev_node.next[level];
            prev_node.next[level] = new_index;
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

    #[test]
    fn get_hits_existing_keys() {
        let mut sl: SkipList<i32, &'static str> = SkipList::new();
        for k in [50, 10, 40, 20, 30, 60, 5, 15] {
            sl.insert(k, "v");
        }
        for k in [50, 10, 40, 20, 30, 60, 5, 15] {
            assert_eq!(sl.get(&k), Some(&"v"), "missed key {k}");
        }
    }

    #[test]
    fn get_returns_none_for_missing_key() {
        let mut sl: SkipList<i32, i32> = SkipList::new();
        for k in [10, 30, 50] {
            sl.insert(k, k);
        }
        // 不存在的 key（含小于最小、大于最大、区间内空隙）。
        assert_eq!(sl.get(&0), None);
        assert_eq!(sl.get(&20), None);
        assert_eq!(sl.get(&100), None);
    }

    #[test]
    fn iterator_is_sorted_and_complete() {
        let mut sl: SkipList<i32, i32> = SkipList::new();
        let mut rng = rand::rng();
        let keys: Vec<i32> = (0..500).map(|_| rng.random_range(0..10_000)).collect();
        for &k in &keys {
            sl.insert(k, k * 2);
        }
        let collected: Vec<(i32, i32)> = sl.iter().map(|(k, v)| (*k, *v)).collect();
        // 数量一致。
        assert_eq!(collected.len(), keys.len());
        // 全局非递减序。
        assert!(collected.windows(2).all(|w| w[0].0 <= w[1].0));
        // value 是 key 的两倍，逐对核对。
        assert!(collected.iter().all(|(k, v)| *v == *k * 2));
    }

    #[test]
    fn iterator_lists_all_duplicate_versions() {
        let mut sl: SkipList<i32, i32> = SkipList::new();
        sl.insert(5, 1);
        sl.insert(5, 2);
        sl.insert(5, 3);
        // 同 key 后插入的排前面：find_prev 用严格 `<` 比较，遇到 == 即停，
        // 新节点插在已有同 key 节点之前。这恰好是 LSM 想要的语义——
        // 新版本（大 seq）排在前，查找时第一个命中即最新。Step 1.4 编码 seq
        // 后这一行为会让大 seq 天然在前。
        let vs: Vec<i32> = sl
            .iter()
            .filter(|(k, _)| *k == &5)
            .map(|(_, v)| *v)
            .collect();
        assert_eq!(vs, vec![3, 2, 1]);
    }

    #[test]
    fn get_returns_latest_version_for_duplicate_keys() {
        let mut sl: SkipList<i32, i32> = SkipList::new();
        sl.insert(5, 1);
        sl.insert(5, 2);
        sl.insert(5, 3);
        // 同 key 多版本时，get 返回最新的（排在最前的）那个。
        assert_eq!(sl.get(&5), Some(&3));
    }
}
