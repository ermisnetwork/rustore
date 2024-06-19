use crate::{sled_table::SledTable, Database};
use anyhow::Result;
use bytes::Bytes;
use sled::{Config, Db};
use ahash::RandomState;
use std::collections::HashMap;


#[derive(Clone)]
pub struct SledDatabase {
    pub db: Db<128>,
    pub local_tables: HashMap<Bytes, SledTable, RandomState>,
}

impl Database for SledDatabase {
    type Item = SledTable;

    fn open_or_create(path: impl AsRef<std::path::Path>) -> Result<Self> {
        let config = Config {
            path: path.as_ref().to_path_buf(),
            flush_every_ms: Some(500),
            cache_capacity_bytes: 1024 * 1024 * 1024,
            ..Default::default()
        };
        let db: Db<128> = config.open()?;
        let local_tables = HashMap::default();
        Ok(Self {
            db,
            local_tables,
        })
    }

    fn open_or_create_table<'a>(&'a mut self, id: Bytes) -> Result<&'a Self::Item> {
        if !self.local_tables.contains_key(&id) {
            let tree = self.db.open_tree(&id)?;
            let table = SledTable {
                tree,
            };
            self.local_tables.insert(id.clone(), table.clone());
            Ok(self.local_tables.get(&id).unwrap())
        }
        else {
            Ok(self.local_tables.get(&id).unwrap())
        }
    }

    fn drop_table(&mut self, id: Bytes) -> Result<()> {
        self.local_tables.remove(&id);
        self.db.drop_tree(id)?;
        Ok(())
    }

    fn flush(&self) -> Result<()> {
        self.db.flush()?;
        for table in self.local_tables.values() {
            table.tree.flush()?;
        }
        Ok(())
    }
}