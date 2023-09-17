use pyo3::prelude::*;
use rand::Rng;
use std::cmp;


#[derive(Debug)]
#[derive(FromPyObject)]
pub struct ResponseTimeTracker {
    pub is_warm_up: bool,
    pub mean: i32,
}

#[derive(Debug)]
#[derive(FromPyObject)]
pub struct DowntimeDetector {
    pub health: i32,
}

#[derive(Debug)]
#[derive(FromPyObject)]
pub struct Server {
    pub response_time_tracker: ResponseTimeTracker,
    pub downtime_detector: DowntimeDetector,
}

pub fn _weighted_sample_ids(weights: Vec<i32>, k: usize) -> Vec<usize> {
    let n: usize = weights.len();
    let mut ids: Vec<usize> = (0..n).collect();

    let mut sum_weight: i32 = weights.iter().sum();

    let mut j: usize = 0;
    for _ in 0..k {
        let pick = rand::thread_rng().gen_range(0..sum_weight);

        let mut i: usize = j;
        let mut sub_sum: i32 = weights[ids[j]];

        while sub_sum < pick {
            i += 1;
            sub_sum += weights[ids[i]];
        }

        sum_weight -= weights[ids[i]];

        let buf: usize = ids[i];
        ids[i] = ids[j];
        ids[j] = buf;

        j += 1;
    }

    let result: Vec<usize> = (&ids[0..k]).to_vec();
    return result;
}

pub fn _get_server_ids(servers: Vec<Server>, max_tries: usize) -> Vec<usize> {
    // println!("servers = {:?}", servers);

    let n: usize = servers.len();
    let count: usize = cmp::min(n, max_tries);

    let mut is_any_warming_up: bool = false;
    let mut min_mean: i32 = 100500;
    let mut max_mean: i32 = 0;

    for server in servers.iter() {
        if server.response_time_tracker.is_warm_up {
            is_any_warming_up = true;
        } else {
            if server.response_time_tracker.mean < min_mean {
                min_mean = server.response_time_tracker.mean;
            }
            if server.response_time_tracker.mean > max_mean {
                max_mean = server.response_time_tracker.mean;
            }
        }
    }

    let mut scores: Vec<i32> = Vec::new();
    for server in servers.iter() {
        let mut time_ms: i32 = 100;
        let mut inverted_time: i32 = time_ms;
        if !is_any_warming_up {
            time_ms = server.response_time_tracker.mean;
            inverted_time = (f64::from(min_mean) * f64::from(max_mean) / f64::from(time_ms)).round() as i32;
        }
        let score: i32 = inverted_time * server.downtime_detector.health;
        scores.push(score)
    }

    return _weighted_sample_ids(scores, count);
}

#[pyfunction]
fn get_server_ids(servers: Vec<Server>, max_tries: usize) -> PyResult<Vec<usize>> {
    let res: Vec<usize> = _get_server_ids(servers, max_tries);
    Ok(res)
}

#[pyfunction]
fn weighted_sample_ids(weights: Vec<i32>, k: usize) -> PyResult<Vec<usize>> {
    let res = _weighted_sample_ids(weights, k);
    Ok(res)
}

/// A Python module implemented in Rust.
#[pymodule]
fn rustik(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(weighted_sample_ids, m)?)?;
    m.add_function(wrap_pyfunction!(get_server_ids, m)?)?;
    Ok(())
}
