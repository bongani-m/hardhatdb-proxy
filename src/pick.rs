/// `first` keeps new connections on the earliest ready node.
/// `leastconn` chooses the ready node with the fewest open splices.
/// A tie stays with the earlier node.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Balance {
    First,
    LeastConn,
}

#[derive(Clone, Copy, Debug)]
pub struct Ready {
    pub index: usize,
    pub conns: usize,
}

pub fn pick(balance: Balance, ready: &[Ready]) -> Option<usize> {
    match balance {
        Balance::First => ready.first().map(|item| item.index),
        Balance::LeastConn => ready
            .iter()
            .min_by_key(|item| item.conns)
            .map(|item| item.index),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ready(pairs: &[(usize, usize)]) -> Vec<Ready> {
        pairs
            .iter()
            .map(|(index, conns)| Ready {
                index: *index,
                conns: *conns,
            })
            .collect()
    }

    #[test]
    fn first_uses_the_earliest_ready_node() {
        assert_eq!(pick(Balance::First, &ready(&[(1, 0), (2, 0)])), Some(1));
        assert_eq!(pick(Balance::First, &[]), None);
    }

    #[test]
    fn leastconn_breaks_ties_toward_the_earlier_node() {
        let nodes = ready(&[(0, 2), (1, 2), (2, 5)]);
        assert_eq!(pick(Balance::LeastConn, &nodes), Some(0));
        let nodes = ready(&[(0, 3), (1, 1), (2, 1)]);
        assert_eq!(pick(Balance::LeastConn, &nodes), Some(1));
    }
}
