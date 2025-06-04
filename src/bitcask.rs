use std::{collections, usize};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path;
use std::path::PathBuf;
use std::fs;


type KeyIdx = collections::HashMap<Vec<u8>, (u64, u32)>;

pub type Result<T> = std::result::Result<T, std::io::Error>;

const KEY_VAL_COLUMN_LEN: u8 = 4;

const MERGE_FILE_TEMP_EXT: &str = "merge_ext";


struct MiniBitcask {
    key_idx: KeyIdx, // key index in memory
    log: Log,
}
struct Log {
   file: std::fs::File,
   path: path::PathBuf,
}

impl Drop for MiniBitcask {
    fn drop(&mut self) {
        if let Err(error) = self.flush() {
            println!("failed to flush file: {:?}", error);
        }
    }
}

impl MiniBitcask {
    fn new(path: PathBuf) -> Result<Self>{
       
       let log = Log::new(path)?;

       let key_idx = log.load_memory()?;

       Ok(Self { log, key_idx })    
    }

    fn set(&mut self, key: &[u8], val: Vec<u8>) -> Result<()>{
         let (val_pos, val_len) =  self.log.write_one_entry(key, Some(&val))?;
         self.key_idx.insert(key.to_vec(), (val_pos , val_len));
         Ok(())
    }

    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
       if let Some((val_pos,val_len )) = self.key_idx.get(key) {
            let val = self.log.read_value(*val_pos, *val_len)?;
            Ok(Some(val))
       }else {
            Ok(None)
        }

    }

    fn delete(&mut self, key: &[u8])  -> Result<()>{
        self.log.write_one_entry(key, None)?;
        self.key_idx.remove(key);
        Ok(())
    }


    fn merge(&mut self)  -> Result<()>{
        // remove deleted key val pair in file
        let mut merge_path = self.log.path.clone();
        merge_path.set_extension(MERGE_FILE_TEMP_EXT);

        let mut new_log = Log::new(merge_path)?;
        let mut new_key_idx = KeyIdx::new();

        for ( key, (val_pos, val_len)) in self.key_idx.iter() {
            let val = self.log.read_value(*val_pos, *val_len)?;
            let (val_pos, val_len) = new_log.write_one_entry(key, Some(&val))?;
            new_key_idx.insert(key.to_vec(), (val_pos, val_len));
        }
    
        std::fs::rename(&new_log.path, &self.log.path)?;

        self.log = new_log;
        self.key_idx = new_key_idx;

        Ok(())
    }


    fn flush(&mut self) -> Result<()> {
        Ok(self.log.file.sync_all()?)
    }
   
}

impl Log {
    fn new(path: PathBuf) -> Result<Self>{
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let file = std::fs::OpenOptions::new()
                        .read(true)
                        .write(true)
                        .create(true)
                        .open(path.as_path())?;
        let path = path.clone();

        Ok(Self { file, path })
    }

 
    /// -------------------------------------------------------------------------——-
    /// ｜     ksz (4byte)    |    value_sz (4byte)    |    key      |     valie    |
    /// --------------------------------------------------------------------------——
    /// 
    /// 
    /// 
    fn load_memory(&self) -> Result<KeyIdx> {
        let mut key_idx = KeyIdx::new();
        let mut len_buf = [0u8; KEY_VAL_COLUMN_LEN as usize];
        let file_len = self.file.metadata()?.len();
        let mut reader = BufReader::new(&self.file);
        let mut pos = reader.seek(SeekFrom::Start(0))?;

        while pos < file_len {
            let one_enrty = || -> Result<(Vec<u8>, (u64, Option<u32>))> {
                reader.read_exact(&mut len_buf)?;
                let ksz = u32::from_be_bytes(len_buf);
                reader.read_exact(&mut len_buf)?;
                let value_sz_r = match  u32::from_be_bytes(len_buf) {
                    l if l >0 => Some(l),
                    _ => None,
                };
                let value_pos = pos + KEY_VAL_COLUMN_LEN as u64 * 2 + ksz as u64;
                
                let mut key = vec![0u8; ksz as usize];
                reader.read_exact(&mut key)?;
                
                // Do not load value in memeory to save spaces

                Ok((key, (value_pos, value_sz_r)))

            }();

            match one_enrty  {
                Ok((key, (v_pos, Some(v_sz)))) => {
                    key_idx.insert(key, (v_pos, v_sz));
                    //we do not read value from file to memory, so the pos need skip the value 
                    reader.seek_relative(v_sz as i64)?;
                    pos =  v_pos + v_sz as u64;

                },
                Ok((key, (v_pos, None))) => {
                     key_idx.remove(&key);
                     pos =  v_pos ;

                },
                Err(err) => return Err(err),

            }

        }

        Ok(key_idx)
    }

    fn write_one_entry(&mut self, key: &[u8], val: Option<&[u8]>) -> Result<(u64, u32)> {
        let key_len = key.len() as u32;
        let val_len = val.map_or(0, |val| val.len()) as u32;

        let entry_len =  KEY_VAL_COLUMN_LEN as usize * 2 + key_len as usize + val_len as usize;

        let offset = self.file.seek(SeekFrom::End(0)) ?;
        let mut writer = BufWriter::with_capacity(entry_len, &self.file);

        writer.write_all(&key_len.to_be_bytes())?;
        writer.write_all(&val_len.to_be_bytes())?;
        writer.write_all(key)?;
        match val_len {
            l if l > 0 => {
                writer.write_all(val.unwrap().as_ref())?;
            },
            _ => {
                
            }
        };
        writer.flush()?;

        let val_pos = offset + entry_len as u64 - val_len as u64;
        Ok((val_pos, val_len as u32))

    }


    fn read_value(&mut self, val_pos: u64, val_len: u32) -> Result<Vec<u8>>{
        let mut val_buff = vec![0; val_len as usize];
        self.file.seek(SeekFrom::Start(val_pos))?;
        self.file.read_exact(&mut val_buff)?;
        Ok(val_buff)
    }
    
}




#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_read() -> Result<()>{
       let temp = std::env::temp_dir().join("bitcask_test01").join("log");
       if temp.exists() {
            fs::remove_file(temp.clone())?;
        }
       let mut mini_bitcask = MiniBitcask::new(temp.clone())?;
       let _ = mini_bitcask.set(b"CQM", b"handsome".to_vec());
       let val = mini_bitcask.get(b"CQM")?;
       assert_eq!(b"handsome".to_vec(), val.unwrap());
       Ok(())
    }

    #[test]
    fn test_write_reboot_read() -> Result<()> {
        let temp = std::env::temp_dir().join("bitcask_test02").join("log");
        if temp.exists() {
            fs::remove_file(temp.clone())?;
        }
        let mut mini_bitcask = MiniBitcask::new(temp.clone())?;
        let _ = mini_bitcask.set(b"CQM", b"handsome".to_vec());
        drop(mini_bitcask);

        let mut reboot_bitcask = MiniBitcask::new(temp.clone())?;
        let val = reboot_bitcask.get(b"CQM")?;
        assert_eq!(b"handsome".to_vec(), val.unwrap());
        Ok(())
    }

    #[test]
    fn test_write_delete_merge() -> Result<()> {
        let  temp = std::env::temp_dir().join("bitcask_test03").join("log");
        if temp.exists() {
            fs::remove_file(temp.clone())?;
        }
        let mut mini_bitcask = MiniBitcask::new(temp.clone())?;
        let _ = mini_bitcask.set(b"CQM", b"handsome".to_vec());
        
        let mut len =mini_bitcask.log.file.metadata()?.len();

        assert!(len > 0);

        mini_bitcask.delete(b"CQM")?;

        mini_bitcask.merge()?;

        len = mini_bitcask.log.file.metadata()?.len();

        assert!(len == 0);

        Ok(())
    }


     #[test]
    fn test_write_repeat_merge() -> Result<()> {
        let temp = std::env::temp_dir().join("bitcask_test04").join("log");
        if temp.exists() {
            fs::remove_file(temp.clone())?;
        }
        let mut mini_bitcask = MiniBitcask::new(temp.clone())?;
        mini_bitcask.set(b"CQM", b"handsome".to_vec())?;   

        mini_bitcask.set(b"CQM", b"so fuck handsome".to_vec())?;
        
        let mut len = mini_bitcask.log.file.metadata()?.len();

        assert!(len == 19 + 27);

        mini_bitcask.merge()?;

        len = mini_bitcask.log.file.metadata()?.len();

        assert!(len == 27);

        Ok(())
    }


     #[test]
    fn test_write_repeat_merge_read() -> Result<()> {
        let temp = std::env::temp_dir().join("bitcask_test05").join("log");
        if temp.exists() {
            fs::remove_file(temp.clone())?;
        }
        let mut mini_bitcask = MiniBitcask::new(temp.clone())?;
        mini_bitcask.set(b"CQM", b"handsome".to_vec())?;   
        let mut val: Option<Vec<u8>> = mini_bitcask.get(b"CQM")?;


        assert_eq!(val.unwrap(), b"handsome".to_vec());

        mini_bitcask.set(b"CQM", b"so fuck handsome".to_vec())?;

        val = mini_bitcask.get(b"CQM")?;
        
        assert_eq!(val.unwrap(), b"so fuck handsome".to_vec());

        mini_bitcask.merge()?;

        val = mini_bitcask.get(b"CQM")?;

        assert_eq!(val.unwrap(), b"so fuck handsome".to_vec());
        Ok(())
    }

}