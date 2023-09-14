use clap::{arg, command, value_parser};

use ego_tree::iter::Children;

use encoding::all::WINDOWS_949;
use encoding::{DecoderTrap, Encoding};

use rust_decimal::prelude::*;

use scraper::{ElementRef, Html, Node, Selector};

use serde::{Deserialize, Serialize};

use std::fs::{create_dir_all, rename, File};
use std::num::ParseIntError;
use std::path::PathBuf;
use std::str::FromStr;
use std::string::{ParseError, String};
use std::thread::sleep;
use std::time::Duration;

#[derive(Serialize, Deserialize)]
struct CrawlResult {
    observed_at: String,
    records: Vec<Record>,
}

#[derive(Clone, Serialize, Deserialize)]
struct Record {
    id: u32,
    name: String,
    height: Option<Height>,
    rain: Rain,
    temperature: Option<Decimal>,
    wind1: Wind,
    wind10: Wind,
    humidity: Option<Decimal>,
    atmospheric: Option<Decimal>,
    address: String,
}

#[derive(Clone, Serialize, Deserialize)]
struct Rain {
    is_raining: RainStatus,
    rain15: Option<Decimal>,
    rain60: Option<Decimal>,
    rain3h: Option<Decimal>,
    rain6h: Option<Decimal>,
    rain12h: Option<Decimal>,
    rainday: Option<Decimal>,
}

#[derive(Clone, Serialize, Deserialize)]
struct Wind {
    direction_code: Option<Decimal>,
    direction_text: WindDirectionText,
    velocity: Option<Decimal>,
}

#[derive(Clone, Serialize, Deserialize)]
struct Height(u32);
impl FromStr for Height {
    type Err = ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s[..s.len() - 1].parse::<u32>() {
            Ok(num) => Ok(Height(num)),
            Err(e) => Err(ParseIntError::from(e)),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
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

#[derive(Clone, Serialize, Deserialize)]
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = "https://www.kma.go.kr/cgi-bin/aws/nph-aws_txt_min";
    let matches = command!()
        .arg(arg!(<base> "base path to store result json").value_parser(value_parser!(PathBuf)))
        .get_matches();
    let base = matches.get_one::<PathBuf>("base").unwrap();
    let mut limit = 5;
    while limit > 0 {
        let resp = reqwest::get(url).await;
        if let Ok(r) = resp {
            if r.status().is_success() {
                let bytes = r.bytes().await?;
                let blob = bytes.as_ref();
                match WINDOWS_949.decode(&blob, DecoderTrap::Ignore) {
                    Err(_) => false,
                    Ok(html) => parse_html(base, &html),
                };
                break;
            }
        }
        sleep(Duration::from_millis(500));
        limit -= 1;
    }

    Ok(())
}

fn parse_html(base: &PathBuf, html: &String) -> bool {
    let document = Html::parse_document(html);
    let time_selector = Selector::parse("span.ehead").unwrap();
    let row_selector = Selector::parse("table table tr").unwrap();
    let dt = document
        .select(&time_selector)
        .next()
        .unwrap()
        .text()
        .next()
        .unwrap();
    let re = regex::Regex::new(
        r"(?P<year>\d{4})\.(?P<month>\d{2})\.(?P<day>\d{2})\.(?P<hour>\d{2}):(?P<minute>\d{2})$",
    )
    .unwrap();
    let cap = re.captures(dt).unwrap();
    let observed_at = format!(
        "{}-{}-{}T{}:{}:00+0900",
        &cap["year"], &cap["month"], &cap["day"], &cap["hour"], &cap["minute"],
    );
    let mut records: Vec<Record> = Vec::new();
    for el in document.select(&row_selector) {
        match make_record(el) {
            Some(record) => records.push(record.clone()),
            None => continue,
        };
    }
    let result = CrawlResult {
        observed_at: observed_at.to_owned(),
        records,
    };
    match write_result_files(base, &result) {
        Ok(_) => println!("done"),
        Err(e) => println!("error: {:?}", e),
    };

    true
}

fn to_decimal_or_none(input: &str) -> Option<Decimal> {
    match Decimal::from_str(input) {
        Ok(x) => Some(x),
        Err(_) => None,
    }
}

fn make_record(el: ElementRef) -> Option<Record> {
    let mut children = el.children();
    let mut cell: [&str; 20] = [""; 20];
    for i in 0..20 {
        cell[i] = get(&mut children)?;
    }

    let id = u32::from_str(cell[0]).unwrap_or(0);
    let name = cell[1].into();
    let height = Height::from_str(cell[2]).ok();
    let rain = Rain {
        is_raining: RainStatus::from_str(cell[3]).ok()?,
        rain15: to_decimal_or_none(cell[4]),
        rain60: to_decimal_or_none(cell[5]),
        rain3h: to_decimal_or_none(cell[6]),
        rain6h: to_decimal_or_none(cell[7]),
        rain12h: to_decimal_or_none(cell[8]),
        rainday: to_decimal_or_none(cell[9]),
    };
    let temperature = to_decimal_or_none(cell[10]);
    let wind1 = Wind {
        direction_code: to_decimal_or_none(cell[11]),
        direction_text: WindDirectionText::from_str(cell[12]).ok()?,
        velocity: to_decimal_or_none(cell[13]),
    };
    let wind10 = Wind {
        direction_code: to_decimal_or_none(cell[14]),
        direction_text: WindDirectionText::from_str(cell[15]).ok()?,
        velocity: to_decimal_or_none(cell[16]),
    };
    let humidity = to_decimal_or_none(cell[17]);
    let atmospheric = to_decimal_or_none(cell[18]);
    let address = cell[19].into();
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
    Some(
        ElementRef::wrap(children.next()?)?
            .text()
            .next()?
            .trim()
            .into(),
    )
}

fn write_result_files(path: &PathBuf, result: &CrawlResult) -> std::io::Result<()> {
    create_dir_all(path)?;
    let mut file = File::create(path.with_file_name(&result.observed_at))?;
    serde_json::to_writer(&mut file, result)?;
    file.sync_all()?;
    rename(
        path.with_file_name(&result.observed_at),
        path.with_file_name("index.json"),
    )?;
    Ok(())
}
