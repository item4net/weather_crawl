#[macro_use]
extern crate serde_derive;

extern crate clap;
extern crate ego_tree;
extern crate encoding;
extern crate regex;
extern crate reqwest;
extern crate scraper;
extern crate serde;
extern crate serde_json;

use clap::{Arg, App};

use ego_tree::iter::Children;

use encoding::{Encoding, DecoderTrap};
use encoding::all::WINDOWS_949;

use regex::Regex;

use scraper::{ElementRef, Html, Node, Selector};

use std::fs::{File, create_dir_all};
use std::num::ParseIntError;
use std::str::FromStr;
use std::string::{ParseError, String};
use std::thread::sleep;
use std::time::Duration;

#[derive(Serialize, Deserialize)]
struct Record {
    id: u32,
    name: String,
    height: Option<Height>,
    rain: Rain,
    temperature: Option<f32>,
    wind1: Wind,
    wind10: Wind,
    humidity: Option<u32>,
    atmospheric: Option<f32>,
    address: String,
}

#[derive(Serialize, Deserialize)]
struct Rain {
    is_raining: RainStatus,
    rain15: Option<f32>,
    rain60: Option<f32>,
    rain3h: Option<f32>,
    rain6h: Option<f32>,
    rain12h: Option<f32>,
    rainday: Option<f32>,
}

#[derive(Serialize, Deserialize)]
struct Wind {
    direction_code: Option<f32>,
    direction_text: WindDirectionText,
    velocity: Option<f32>,
}

#[derive(Serialize, Deserialize)]
struct Height(u32);
impl FromStr for Height {
    type Err = ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s[..s.len()-1].parse::<u32>() {
            Ok(num) => Ok(Height(num)),
            Err(e) => Err(ParseIntError::from(e)),
        }
    }
}

#[derive(Serialize, Deserialize)]
enum RainStatus {
    Clear,
    Rain,
    Unavailable,
    Unknown,
}
impl FromStr for RainStatus {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "●" => RainStatus::Rain,
            "○" => RainStatus::Clear,
            "." => RainStatus::Unavailable,
            _ => RainStatus::Unknown,
        })
    }
}

#[derive(Serialize, Deserialize)]
enum WindDirectionText {
    N,
    NNW,
    NW,
    WNW,
    W,
    WSW,
    SW,
    SSW,
    S,
    SSE,
    SE,
    ESE,
    E,
    ENE,
    NE,
    NNE,
    No,
    Unavailable,
}
impl FromStr for WindDirectionText {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "N" => WindDirectionText::N,
            "NNW" => WindDirectionText::NNW,
            "NW" => WindDirectionText::NW,
            "WNW" => WindDirectionText::WNW,
            "W" => WindDirectionText::W,
            "WSW" => WindDirectionText::WSW,
            "SW" => WindDirectionText::SW,
            "SSW" => WindDirectionText::SSW,
            "S" => WindDirectionText::S,
            "SSE" => WindDirectionText::SSE,
            "SE" => WindDirectionText::SE,
            "ESE" => WindDirectionText::ESE,
            "E" => WindDirectionText::E,
            "ENE" => WindDirectionText::ENE,
            "NE" => WindDirectionText::NE,
            "NNE" => WindDirectionText::NNE,
            "-" => WindDirectionText::No,
            _ => WindDirectionText::Unavailable,
        })
    }
}

fn main() -> Result<(), Box<std::error::Error>> {
    let url = "http://www.kma.go.kr/cgi-bin/aws/nph-aws_txt_min";
    let matches = App::new("aws-crawl")
        .arg(Arg::with_name("BASE")
                 .required(true)
                 .takes_value(true)
                 .index(1)
                 .help("base path to store result json."))
        .get_matches();
    let base = matches.value_of("BASE").unwrap();
    loop {
        let resp = reqwest::get(url);
        if let Ok(mut r) = resp {
            if handle_response(base, &mut r) {
                break;
            }
        }
        sleep(Duration::from_millis(500))
    }
   
    Ok(())
}

fn handle_response(base: &str, resp: &mut reqwest::Response) -> bool {
    if resp.status().is_success() {
        let mut buf: Vec<u8> = vec![];
        if let Err(_) = resp.copy_to(&mut buf) {
            return false;
        }
        match WINDOWS_949.decode(&buf, DecoderTrap::Ignore) {
            Err(_) => return false,
            Ok(html) => {
                parse_html(base, &html);
                return true;
            },
        }
    }
    false
}

fn parse_html(base: &str, html: &String) {
    let document = Html::parse_document(html);
    let time_selector = Selector::parse("span.ehead").unwrap();
    let row_selector = Selector::parse("table table tr").unwrap();
    let dt = document.select(&time_selector).next().unwrap().text().next().unwrap();
    let re = Regex::new(r"(?P<year>\d{4})\.(?P<month>\d{2})\.(?P<day>\d{2})\.(?P<hour>\d{2}):(?P<minute>\d{2})$").unwrap();
    let cap = re.captures(dt).unwrap();
    let dt_path = format!(
        "{}/{}/{}/{}/{}",
        &cap["year"],
        &cap["month"],
        &cap["day"],
        &cap["hour"],
        &cap["minute"],
    );
    let dt_path = dt_path.as_str();
    let mut results: Vec<Record> = vec![];
    for el in document.select(&row_selector) {
        match make_record(el) {
            Some(r) => results.push(r),
            None => continue,
        };
    }
    match write_files(base, dt_path, &results) {
        Ok(_) => println!("done: {}", dt_path),
        Err(e) => println!("error: {:?}", e),
    };
}

fn make_record(el: ElementRef) -> Option<Record> {
    let mut children = el.children();
    let id = get(&mut children)?.parse().ok()?;
    let name = get(&mut children)?.into();
    let height = get(&mut children)?.parse().ok();
    let rain = Rain {
        is_raining: get(&mut children)?.parse().ok()?,
        rain15: get(&mut children)?.parse().ok(),
        rain60: get(&mut children)?.parse().ok(),
        rain3h: get(&mut children)?.parse().ok(),
        rain6h: get(&mut children)?.parse().ok(),
        rain12h: get(&mut children)?.parse().ok(),
        rainday: get(&mut children)?.parse().ok(),
    };
    let temperature: Option<f32> = get(&mut children)?.parse().ok();
    let wind1 = Wind {
        direction_code: get(&mut children)?.parse().ok(),
        direction_text: get(&mut children)?.parse().unwrap(),
        velocity: get(&mut children)?.parse().ok(),
    };
    let wind10 = Wind {
        direction_code: get(&mut children)?.parse().ok(),
        direction_text: get(&mut children)?.parse().unwrap(),
        velocity: get(&mut children)?.parse().ok(),
    };
    let humidity = get(&mut children)?.parse().ok();
    let atmospheric = get(&mut children)?.parse().ok();
    let address = get(&mut children)?.into();
    Some(Record {
        id,
        name,
        height,
        rain,
        temperature,
        wind1,
        wind10,
        humidity,
        atmospheric,
        address,
    })
}

fn get<'a>(children: &mut Children<'a, Node>) -> Option<&'a str> {
    Some(ElementRef::wrap(children.next()?)?.text().next()?.into())
}

fn write_files(path: &str, dt_path: &str, results: &Vec<Record>) -> std::io::Result<()> {
    create_dir_all(format!("{}/{}", path, dt_path))?;
    let file = File::create(format!("{}/{}/index.json", path, dt_path))?;
    serde_json::to_writer(file, results)?;

    create_dir_all(format!("{}/latest", path))?;
    let file = File::create(format!("{}/latest/index.json", path))?;
    serde_json::to_writer(file, results)?;
    Ok(())
}