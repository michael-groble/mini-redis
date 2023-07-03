use crate::cmd::Parse;
use crate::{Connection, Db, Frame};

use bytes::Bytes;
use tracing::{debug, instrument};

#[derive(Debug)]
pub struct Zscore {
    key: String,
    member: String,
}

impl Zscore {
    pub fn new(key: impl ToString, member: impl ToString) -> Zscore {
        Zscore {
            key: key.to_string(),
            member: member.to_string(),
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }
    pub fn member(&self) -> &str {
        &self.member
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Zscore> {
        let key = parse.next_string()?;
        let member = parse.next_string()?;

        Ok(Zscore { key, member })
    }

    #[instrument(skip(self, db, dst))]
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let response = if let Some(value) = db.zscore(self.key, self.member) {
            Frame::Bulk(Bytes::from(value.into_bytes()))
        } else {
            Frame::Null
        };

        debug!(?response);
        dst.write_frame(&response).await?;
        Ok(())
    }

    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("Zget".as_bytes()));
        frame.push_bulk(Bytes::from(self.key.into_bytes()));
        frame.push_bulk(Bytes::from(self.member.into_bytes()));
        frame
    }
}
