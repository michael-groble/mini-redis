use crate::cmd::Parse;
use crate::{Connection, Db, Frame};

use bytes::Bytes;
use tracing::{debug, instrument};

#[derive(Debug)]
pub struct Zadd {
    key: String,
    member: String,
    score: f64,
}

impl Zadd {
    pub fn new(key: impl ToString, member: impl ToString, score: f64) -> Zadd {
        Zadd {
            key: key.to_string(),
            member: member.to_string(),
            score,
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }
    pub fn member(&self) -> &str {
        &self.member
    }
    pub fn score(&self) -> f64 {
        self.score
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Zadd> {
        let key = parse.next_string()?;
        let score = parse.next_string()?.parse::<f64>()?;
        let member = parse.next_string()?;

        Ok(Zadd { key, member, score })
    }

    #[instrument(skip(self, db, dst))]
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let count = db.zadd(self.key, self.member, self.score);
        let response = Frame::Integer(count as u64);
        debug!(?response);
        dst.write_frame(&response).await?;
        Ok(())
    }

    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("zadd".as_bytes()));
        frame.push_bulk(Bytes::from(self.key.into_bytes()));
        frame.push_bulk(Bytes::from(self.score.to_string().into_bytes()));
        frame.push_bulk(Bytes::from(self.member.into_bytes()));
        frame
    }
}
