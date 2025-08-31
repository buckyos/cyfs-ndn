use crate::object_map::{SimpleObjectMap, SimpleMapItem};
use crate::object::ObjId;

/// 基础结构体 - 相当于"父类"
#[derive(Debug, Clone)]
struct BaseMap {
    data: std::collections::HashMap<String, String>,
    count: u64,
}

impl BaseMap {
    fn new() -> Self {
        Self {
            data: std::collections::HashMap::new(),
            count: 0,
        }
    }
    
    fn insert(&mut self, key: String, value: String) -> Option<String> {
        let old_value = self.data.insert(key, value);
        if old_value.is_none() {
            self.count += 1;
        }
        old_value
    }
    
    fn get(&self, key: &str) -> Option<&String> {
        self.data.get(key)
    }
    
    fn len(&self) -> usize {
        self.data.len()
    }
    
    fn count(&self) -> u64 {
        self.count
    }
}

/// 派生结构体 - 相当于"子类"
#[derive(Debug, Clone)]
struct DerivedMap {
    // 组合基础结构体
    base: BaseMap,
    // 额外的字段
    name: String,
    metadata: std::collections::HashMap<String, String>,
}

impl DerivedMap {
    fn new(name: String) -> Self {
        Self {
            base: BaseMap::new(),
            name,
            metadata: std::collections::HashMap::new(),
        }
    }
    
    // 委托方法 - 相当于"继承"父类的方法
    fn insert(&mut self, key: String, value: String) -> Option<String> {
        self.base.insert(key, value)  // 委托给 BaseMap::insert()
    }
    
    fn get(&self, key: &str) -> Option<&String> {
        self.base.get(key)  // 委托给 BaseMap::get()
    }
    
    fn len(&self) -> usize {
        self.base.len()  // 委托给 BaseMap::len()
    }
    
    fn count(&self) -> u64 {
        self.base.count()  // 委托给 BaseMap::count()
    }
    
    // 派生类特有的方法
    fn set_metadata(&mut self, key: String, value: String) {
        self.metadata.insert(key, value);
    }
    
    fn get_metadata(&self, key: &str) -> Option<&String> {
        self.metadata.get(key)
    }
    
    fn name(&self) -> &str {
        &self.name
    }
}

/// 展示组合+委托模式的使用
pub fn demonstrate_composition_delegation() {
    println!("=== Rust 组合+委托模式演示 ===");
    
    // 创建基础对象
    let mut base = BaseMap::new();
    base.insert("key1".to_string(), "value1".to_string());
    base.insert("key2".to_string(), "value2".to_string());
    
    println!("BaseMap: len={}, count={}", base.len(), base.count());
    
    // 创建派生对象
    let mut derived = DerivedMap::new("my_derived_map".to_string());
    
    // 使用委托的方法（相当于继承的方法）
    derived.insert("key1".to_string(), "value1".to_string());
    derived.insert("key2".to_string(), "value2".to_string());
    derived.insert("key3".to_string(), "value3".to_string());
    
    println!("DerivedMap: len={}, count={}", derived.len(), derived.count());
    println!("DerivedMap name: {}", derived.name());
    
    // 使用派生类特有的方法
    derived.set_metadata("owner".to_string(), "user123".to_string());
    derived.set_metadata("created".to_string(), "2024-01-01".to_string());
    
    if let Some(owner) = derived.get_metadata("owner") {
        println!("Owner: {}", owner);
    }
    
    // 验证委托是否正常工作
    if let Some(value) = derived.get("key1") {
        println!("Retrieved value: {}", value);
    }
    
    println!("=== 演示完成 ===\n");
}

/// 对比 SimpleObjectMap 和 DirObject 的继承关系
pub fn demonstrate_object_map_inheritance() {
    println!("=== ObjectMap 继承关系演示 ===");
    
    // 创建 SimpleObjectMap
    let mut simple_map = SimpleObjectMap::new();
    let obj_id1 = ObjId::default();
    let obj_id2 = ObjId::default();
    
    simple_map.insert("file1.txt".to_string(), SimpleMapItem::ObjId(obj_id1.clone()));
    simple_map.insert("file2.txt".to_string(), SimpleMapItem::ObjId(obj_id2.clone()));
    
    println!("SimpleObjectMap: len={}", simple_map.len());
    
    // 注意：由于 DirObject 被删除了，这里只是展示概念
    println!("DirObject 会通过组合 SimpleObjectMap 来继承其功能");
    println!("DirObject 会委托以下方法到 SimpleObjectMap:");
    println!("  - len()");
    println!("  - get()");
    println!("  - insert()");
    println!("  - remove()");
    println!("  - contains_key()");
    println!("  - keys()");
    println!("  - values()");
    println!("  - iter()");
    
    println!("=== 演示完成 ===\n");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_base_map() {
        let mut base = BaseMap::new();
        base.insert("key1".to_string(), "value1".to_string());
        assert_eq!(base.len(), 1);
        assert_eq!(base.count(), 1);
        assert_eq!(base.get("key1"), Some(&"value1".to_string()));
    }

    #[test]
    fn test_derived_map() {
        let mut derived = DerivedMap::new("test".to_string());
        derived.insert("key1".to_string(), "value1".to_string());
        assert_eq!(derived.len(), 1);
        assert_eq!(derived.count(), 1);
        assert_eq!(derived.get("key1"), Some(&"value1".to_string()));
        assert_eq!(derived.name(), "test");
    }

    #[test]
    fn test_delegation() {
        let mut derived = DerivedMap::new("test".to_string());
        
        // 测试委托的方法
        derived.insert("key1".to_string(), "value1".to_string());
        derived.insert("key2".to_string(), "value2".to_string());
        
        assert_eq!(derived.len(), 2);
        assert_eq!(derived.count(), 2);
        assert_eq!(derived.get("key1"), Some(&"value1".to_string()));
        assert_eq!(derived.get("key2"), Some(&"value2".to_string()));
        
        // 测试派生类特有的方法
        derived.set_metadata("owner".to_string(), "user123".to_string());
        assert_eq!(derived.get_metadata("owner"), Some(&"user123".to_string()));
    }
}
