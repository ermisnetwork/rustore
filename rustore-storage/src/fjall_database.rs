use std::collections::HashMap;
use ahash::RandomState;
use fjall::{Config, Keyspace, PersistMode};
use anyhow::Result;
use bytes::Bytes;

use crate::{fjall_table::FjallTable, Database};
#[derive(Clone)]
pub struct FjallDatabase {
    pub db: Keyspace,
    pub local_tables: HashMap<Bytes, FjallTable, RandomState>,
}

impl Database for FjallDatabase {
    type Item = FjallTable;

    fn open_or_create(path: impl AsRef<std::path::Path>) -> Result<Self> {
        let db = Config::new(path.as_ref()).open()?;
        let local_tables = HashMap::default();
        Ok(Self {
            db,
            local_tables,
        })
    }

    fn open_or_create_table<'a>(&'a mut self, id: Bytes) -> Result<&'a Self::Item> {
        if !self.local_tables.contains_key(&id) {
            let id_str = String::from_utf8(id.to_vec())?;
            let tree = self.db.open_partition(id_str.as_str(), Default::default())?;
            let table = FjallTable {
                tree,
                db_handle: self.db.clone(),
            };
            self.local_tables.insert(id.clone(), table.clone());
            Ok(self.local_tables.get(&id).unwrap())
        }
        else {
            Ok(self.local_tables.get(&id).unwrap())
        }
    }

    fn drop_table(&mut self, id: Bytes) -> Result<()> {
        if let Some(table) = self.local_tables.remove(&id){
            self.db.delete_partition(table.tree)?;
        }
        Ok(())
    }

    fn flush(&self) -> Result<()> {
        self.db.persist(PersistMode::SyncData)?;
        Ok(())
    }
}