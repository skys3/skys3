//! Checks the metrics reference (`docs/skys3-metrics.md`) against every
//! metric the code registers, and the alerting rules and the Grafana
//! dashboard under `deploy/` against the reference.
//!
//! The catalog registers the metrics of every component in one registry,
//! as a node does, without running a node. A scan of the crates' sources
//! makes sure the catalog misses no component: every metric registered
//! anywhere must be in it.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use skys3_coord::CoordinatorMetrics;
use skys3_ec::RepairMetrics;
use skys3_flush::{DirtyBudget, FlushMetrics};
use skys3_gateway::HotCacheMetrics;
use skys3_obs::{AdminConfig, AdminListener, Health, MetricsRegistry};
use skys3_shard::lifecycle::LifecycleMetrics;
use skys3_shard::replication::ReplicationMetrics;
use skys3_shard::{CacheMetrics, CompactionMetrics};
use skys3_types::Label;
use yaml_rust2::{Yaml, YamlLoader};

use super::NodeMetrics;
use crate::admission::{DiskOf, DiskSpace, NodeAdmission};

const REFERENCE: &str = include_str!("../../../../docs/skys3-metrics.md");
const DESIGN: &str = include_str!("../../../../docs/skys3-design.md");
const ALERTS: &str = include_str!("../../../../deploy/prometheus/skys3-alerts.yml");
const DASHBOARD: &str = include_str!("../../../../deploy/grafana/skys3-dashboard.json");

/// Where every alert's runbook lives; each alert names a section of it.
const RUNBOOKS: &str = "https://github.com/skys3/skys3/blob/main/docs/skys3-runbooks.md#";

/// Series that alerts and the dashboard use but no SkyS3 node exports: the
/// scrape health Prometheus records, and node_exporter's clock state.
const EXTERNAL: &[&str] = &["up", "node_timex_sync_status"];

/// Every metric the code registers, each component registering its own
/// metrics in one registry, as a node does.
async fn catalog() -> MetricsRegistry {
    let registry = NodeMetrics::new(Duration::from_secs(3600)).registry;
    let admin = AdminConfig {
        listen: ([127, 0, 0, 1], 0).into(),
        token: None,
    };
    drop(
        AdminListener::bind(admin, registry.clone(), Health::new())
            .await
            .unwrap(),
    );
    let disk_of: DiskOf = Box::new(|_| Label::new("disk-0").unwrap());
    drop(NodeAdmission::new(
        Arc::new(DirtyBudget::unlimited()),
        Arc::new(DiskSpace::new(0)),
        disk_of,
        &registry,
    ));
    drop(FlushMetrics::register(&registry));
    drop(ReplicationMetrics::register(&registry));
    drop(CacheMetrics::register(&registry));
    drop(CompactionMetrics::register(&registry));
    drop(HotCacheMetrics::register(&registry));
    drop(LifecycleMetrics::register(&registry));
    drop(RepairMetrics::register(&registry));
    drop(CoordinatorMetrics::register(&registry));
    registry
}

/// A metric family as the registry exports it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Family {
    /// The family's name, without a type suffix.
    name: String,
    /// `counter`, `gauge`, `histogram`, or `info`.
    kind: String,
}

impl Family {
    /// The name the reference lists: with `_total` for a counter and
    /// `_info` for an info metric, as the exporter writes samples.
    fn exported(&self) -> String {
        match self.kind.as_str() {
            "counter" => format!("{}_total", self.name),
            "info" => format!("{}_info", self.name),
            _ => self.name.clone(),
        }
    }

    /// The names of the samples the family exports.
    fn series(&self) -> Vec<String> {
        match self.kind.as_str() {
            "histogram" => ["_bucket", "_sum", "_count"]
                .iter()
                .map(|suffix| format!("{}{suffix}", self.name))
                .collect(),
            _ => vec![self.exported()],
        }
    }
}

/// The families of the catalog, by exported name.
async fn families() -> BTreeMap<String, Family> {
    let mut families = BTreeMap::new();
    for registered in catalog().await.registered() {
        let family = Family {
            name: registered.family,
            kind: registered.kind,
        };
        let exported = family.exported();
        assert!(
            families.insert(exported.clone(), family).is_none(),
            "{exported} is registered twice"
        );
    }
    families
}

/// A metric row of the reference's section 3.
#[derive(Debug)]
struct Row {
    name: String,
    kind: String,
    labels: String,
    design: String,
    status: String,
}

impl Row {
    fn planned(&self) -> bool {
        self.status.starts_with("planned")
    }
}

/// The lines of the reference section whose heading starts with `heading`,
/// up to the next heading of the same level.
fn section<'a>(document: &'a str, heading: &str) -> Vec<&'a str> {
    let level = heading.split(' ').next().unwrap();
    let mut lines = document
        .lines()
        .skip_while(|line| !line.starts_with(heading));
    let first = lines
        .next()
        .unwrap_or_else(|| panic!("no section {heading}"));
    std::iter::once(first)
        .chain(
            lines.take_while(|line| {
                !(line.starts_with(level) && line[level.len()..].starts_with(' '))
            }),
        )
        .collect()
}

/// The cells of a Markdown table row, trimmed.
fn cells(line: &str) -> Vec<&str> {
    let inner = line.trim().trim_start_matches('|').trim_end_matches('|');
    inner.split('|').map(str::trim).collect()
}

/// The rows of every table in `lines` whose first cell is in backticks
/// and starts with `prefix`.
fn table_rows<'a>(lines: &[&'a str], prefix: &str) -> Vec<Vec<&'a str>> {
    lines
        .iter()
        .filter(|line| line.starts_with(&format!("| `{prefix}")))
        .map(|line| cells(line))
        .collect()
}

/// The metric rows of the reference, by name.
fn reference() -> BTreeMap<String, Row> {
    let mut rows = BTreeMap::new();
    for cells in table_rows(&section(REFERENCE, "## 3. Metrics"), "skys3_") {
        assert_eq!(cells.len(), 6, "a metric row has six cells: {cells:?}");
        let row = Row {
            name: cells[0].trim_matches('`').to_owned(),
            kind: cells[1].to_owned(),
            labels: cells[2].to_owned(),
            design: cells[3].to_owned(),
            status: cells[4].to_owned(),
        };
        assert!(!cells[5].is_empty(), "{} has no description", row.name);
        let name = row.name.clone();
        assert!(
            rows.insert(name.clone(), row).is_none(),
            "{name} is listed twice"
        );
    }
    rows
}

/// The names in backticks in `text`.
fn quoted(text: &str) -> Vec<&str> {
    text.split('`').skip(1).step_by(2).collect()
}

/// The section numbers of the design's headings, such as `6.4` and `13`.
fn design_sections() -> BTreeSet<String> {
    DESIGN
        .lines()
        .filter(|line| line.starts_with("## ") || line.starts_with("### "))
        .filter_map(|line| line.split(' ').nth(1))
        .map(|number| number.trim_end_matches('.').to_owned())
        .collect()
}

#[tokio::test]
async fn the_reference_lists_every_registered_metric_and_nothing_else() {
    let families = families().await;
    let rows = reference();
    for (name, family) in &families {
        let row = rows
            .get(name)
            .unwrap_or_else(|| panic!("{name} is registered but not in docs/skys3-metrics.md"));
        assert_eq!(row.kind, family.kind, "the type of {name}");
        assert!(!row.planned(), "{name} is registered, so it is not planned");
        assert!(!row.labels.is_empty(), "{name} lists no labels (say none)");
    }
    for (name, row) in &rows {
        if row.planned() {
            continue;
        }
        assert!(
            families.contains_key(name),
            "docs/skys3-metrics.md lists {name} ({}), which no code registers",
            row.status
        );
    }
}

#[test]
fn every_metric_names_the_design_sections_it_implements() {
    let sections = design_sections();
    for row in reference().values() {
        let cited: Vec<&str> = row.design.split(", ").collect();
        for cited in cited {
            let number = cited.strip_prefix('§').unwrap_or_else(|| {
                panic!("{}: design column {:?} is not §N.N", row.name, row.design)
            });
            assert!(
                sections.contains(number),
                "{} cites §{number}, which the design does not have",
                row.name
            );
        }
    }
}

/// The metric base names that sources under `dir` register, with the file
/// each is registered in. Test modules at the end of a file, comment
/// lines, and test-only files are skipped.
fn registered_in_sources(dir: &Path, found: &mut Vec<(String, String)>) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        if path.is_dir() {
            if name != "tests" && name != "target" {
                registered_in_sources(&path, found);
            }
            continue;
        }
        if !name.ends_with(".rs") || name == "tests.rs" {
            continue;
        }
        let text = std::fs::read_to_string(&path).unwrap();
        let code = text
            .find("#[cfg(test)]\nmod tests")
            .map_or(text.as_str(), |end| &text[..end]);
        let code: String = code
            .lines()
            .filter(|line| !line.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n");
        for call in [".register(", ".register_with_unit("] {
            for (at, _) in code.match_indices(call) {
                let rest = code[at + call.len()..].trim_start();
                let Some(rest) = rest.strip_prefix('"') else {
                    continue;
                };
                let Some((base, rest)) = rest.split_once('"') else {
                    continue;
                };
                // A metric registration has a help text after the name; a
                // health component's has nothing.
                let name = base
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
                if name && rest.trim_start().starts_with(',') {
                    found.push((base.to_owned(), path.display().to_string()));
                }
            }
        }
    }
}

#[tokio::test]
async fn the_catalog_has_every_metric_the_sources_register() {
    let families = families().await;
    let crates = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    let mut found = Vec::new();
    registered_in_sources(&crates, &mut found);
    assert!(found.len() > 50, "the scan found only {found:?}");
    for (base, file) in found {
        let name = format!("skys3_{base}");
        let known = families.values().any(|family| {
            family.name == name
                || family
                    .name
                    .strip_prefix(&format!("{name}_"))
                    .is_some_and(|unit| unit.chars().all(|c| c.is_ascii_lowercase()))
        });
        assert!(
            known,
            "{file} registers {base:?}, which the catalog in {} does not build",
            file!()
        );
    }
}

/// The metric names a PromQL expression reads, without label matchers,
/// ranges, strings, functions, keywords, and the label lists of grouping
/// and matching modifiers.
fn metric_names(expr: &str) -> BTreeSet<String> {
    const KEYWORDS: &[&str] = &[
        // Operators and modifiers.
        "and",
        "or",
        "unless",
        "bool",
        "offset",
        "inf",
        "nan",
        "by",
        "without",
        "on",
        "ignoring",
        "group_left",
        "group_right",
        // Aggregation operators, which may be followed by a modifier
        // rather than their parenthesis.
        "sum",
        "min",
        "max",
        "avg",
        "group",
        "stddev",
        "stdvar",
        "count",
        "count_values",
        "bottomk",
        "topk",
        "quantile",
    ];
    const MODIFIERS: &[&str] = &[
        "by",
        "without",
        "on",
        "ignoring",
        "group_left",
        "group_right",
    ];
    let chars: Vec<char> = expr.chars().collect();
    let mut names = BTreeSet::new();
    let mut i = 0;
    let skip_to = |from: usize, close: char| {
        let mut j = from;
        while j < chars.len() && chars[j] != close {
            j += 1;
        }
        j + 1
    };
    while i < chars.len() {
        let c = chars[i];
        match c {
            '{' => i = skip_to(i, '}'),
            '[' => i = skip_to(i, ']'),
            '"' | '\'' => i = skip_to(i + 1, c),
            c if c.is_ascii_digit() || c == '.' => {
                while i < chars.len() && (chars[i].is_ascii_alphanumeric() || chars[i] == '.') {
                    i += 1;
                }
            }
            c if c.is_ascii_alphabetic() || c == '_' || c == ':' => {
                let start = i;
                while i < chars.len()
                    && (chars[i].is_ascii_alphanumeric() || chars[i] == '_' || chars[i] == ':')
                {
                    i += 1;
                }
                let word: String = chars[start..i].iter().collect();
                let mut next = i;
                while next < chars.len() && chars[next].is_whitespace() {
                    next += 1;
                }
                let call = chars.get(next) == Some(&'(');
                if MODIFIERS.contains(&word.as_str()) && call {
                    i = skip_to(next, ')');
                } else if !call && !KEYWORDS.contains(&word.as_str()) {
                    names.insert(word);
                }
            }
            _ => i += 1,
        }
    }
    names
}

/// Every series name a query may read: the samples of each family, and
/// the external series.
async fn readable() -> BTreeSet<String> {
    families()
        .await
        .values()
        .flat_map(Family::series)
        .chain(EXTERNAL.iter().map(|name| (*name).to_owned()))
        .collect()
}

/// Checks that `expr` reads at least one series, and only known ones.
fn check_query(expr: &str, readable: &BTreeSet<String>, context: &str) {
    let names = metric_names(expr);
    assert!(!names.is_empty(), "{context}: {expr:?} reads no metric");
    for name in names {
        assert!(
            readable.contains(&name),
            "{context}: {expr:?} reads {name}, which is not in docs/skys3-metrics.md"
        );
    }
}

#[test]
fn queries_are_parsed_into_their_metric_names() {
    let names = |expr| metric_names(expr).into_iter().collect::<Vec<_>>();
    assert_eq!(
        names(
            "max by (bucket) (rate(a_total{x=\"y\", z=~\"$v\"}[5m])) > 0.75 * b_seconds \
             and on (instance) c == 1 or time() - d offset 5m"
        ),
        ["a_total", "b_seconds", "c", "d"]
    );
    assert_eq!(
        names("histogram_quantile(0.5, sum by (le) (rate(h_bucket[1h])))"),
        ["h_bucket"]
    );
    assert_eq!(names("sum(x) / ignoring(code) group_left y"), ["x", "y"]);
}

/// A string field of a YAML mapping.
fn field<'a>(yaml: &'a Yaml, key: &str, context: &str) -> &'a str {
    yaml[key]
        .as_str()
        .unwrap_or_else(|| panic!("{context}: no string {key}"))
}

/// Whether `duration` is a Prometheus duration such as `5m` or `0m`.
fn is_duration(duration: &str) -> bool {
    let (number, unit) = duration.split_at(duration.len().saturating_sub(1));
    !number.is_empty()
        && number.chars().all(|c| c.is_ascii_digit())
        && ["s", "m", "h", "d"].contains(&unit)
}

/// An alert's severity, `for` duration, and runbook section (the anchor in
/// `docs/skys3-runbooks.md`).
type AlertEntry = (String, String, String);

/// The alerts of the reference's section 4, by name.
fn reference_alerts() -> BTreeMap<String, AlertEntry> {
    table_rows(&section(REFERENCE, "## 4. Alerts"), "SkyS3")
        .into_iter()
        .map(|cells| {
            assert_eq!(cells.len(), 5, "an alert row has five cells: {cells:?}");
            let entry = (
                cells[1].to_owned(),
                cells[2].to_owned(),
                cells[3].trim_matches('`').to_owned(),
            );
            (cells[0].trim_matches('`').to_owned(), entry)
        })
        .collect()
}

#[tokio::test]
async fn alerting_rules_are_complete_and_read_known_metrics() {
    let readable = readable().await;
    let documents = YamlLoader::load_from_str(ALERTS).unwrap();
    let groups = documents[0]["groups"].as_vec().expect("a list of groups");
    let mut alerts = BTreeMap::new();
    for group in groups {
        let group_name = field(group, "name", "a group");
        let rules = group["rules"].as_vec().expect("a list of rules");
        assert!(!rules.is_empty(), "group {group_name} has no rules");
        for rule in rules {
            let alert = field(rule, "alert", group_name);
            assert!(alert.starts_with("SkyS3"), "{alert}");
            check_query(field(rule, "expr", alert), &readable, alert);
            let duration = field(rule, "for", alert);
            assert!(is_duration(duration), "{alert}: for {duration:?}");
            let severity = field(&rule["labels"], "severity", alert);
            assert!(
                ["critical", "warning"].contains(&severity),
                "{alert}: severity {severity}"
            );
            let annotations = &rule["annotations"];
            for key in ["summary", "description"] {
                assert!(!field(annotations, key, alert).is_empty(), "{alert}: {key}");
            }
            let anchor = field(annotations, "runbook_url", alert)
                .strip_prefix(RUNBOOKS)
                .unwrap_or_else(|| panic!("{alert}: the runbook is not under {RUNBOOKS}"));
            assert!(
                !anchor.is_empty()
                    && anchor
                        .chars()
                        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-'),
                "{alert}: runbook anchor {anchor:?}"
            );
            let entry = (severity.to_owned(), duration.to_owned(), anchor.to_owned());
            assert!(
                alerts.insert(alert.to_owned(), entry).is_none(),
                "{alert} is defined twice"
            );
        }
    }
    assert_eq!(
        alerts,
        reference_alerts(),
        "docs/skys3-metrics.md section 4 lists every alert with its severity, for, and runbook"
    );
}

#[tokio::test]
async fn dashboard_queries_read_known_metrics() {
    let readable = readable().await;
    let dashboard: Value = serde_json::from_str(DASHBOARD).unwrap();
    let mut ids = BTreeSet::new();
    let mut queries = 0;
    let panels = dashboard["panels"].as_array().unwrap();
    for panel in panels {
        let title = panel["title"].as_str().unwrap();
        assert!(!title.is_empty());
        let id = panel["id"].as_u64().unwrap();
        assert!(ids.insert(id), "panel id {id} ({title}) is used twice");
        if panel["type"] == "row" {
            continue;
        }
        assert_eq!(panel["datasource"]["uid"], "${datasource}", "{title}");
        let targets = panel["targets"].as_array().unwrap();
        assert!(!targets.is_empty(), "{title} has no query");
        for target in targets {
            check_query(target["expr"].as_str().unwrap(), &readable, title);
            queries += 1;
        }
    }
    assert!(queries > 40, "{queries} queries");
    for variable in dashboard["templating"]["list"].as_array().unwrap() {
        let Some(query) = variable["definition"].as_str() else {
            continue;
        };
        let metric = query
            .strip_prefix("label_values(")
            .and_then(|rest| rest.split(',').next())
            .unwrap_or_else(|| panic!("variable query {query:?}"));
        assert!(readable.contains(metric), "variable query {query:?}");
    }
}

#[tokio::test]
async fn every_failure_in_the_matrix_has_a_metric_or_an_alert() {
    let rows = reference();
    let alerts = reference_alerts();
    let failures: Vec<&str> = section(DESIGN, "## 13. Failure matrix")
        .iter()
        .filter(|line| line.starts_with("| ") && !line.starts_with("| Failure"))
        .map(|line| cells(line)[0])
        .collect();
    assert!(failures.len() > 10, "{failures:?}");
    let coverage: Vec<Vec<&str>> = section(REFERENCE, "## 6. Failure matrix coverage")
        .iter()
        .filter(|line| line.starts_with("| ") && !line.starts_with("| Failure"))
        .map(|line| cells(line))
        .collect();
    let covered: Vec<&str> = coverage.iter().map(|cells| cells[0]).collect();
    assert_eq!(
        covered, failures,
        "section 6 of docs/skys3-metrics.md has a row for each row of design section 13, in order"
    );
    for cells in coverage {
        assert_eq!(cells.len(), 4, "{cells:?}");
        let (failure, metrics, alerting) = (cells[0], cells[1], cells[2]);
        let metrics = quoted(metrics);
        let alerting = quoted(alerting);
        assert!(
            !metrics.is_empty() || !alerting.is_empty(),
            "{failure}: no metric or alert shows it"
        );
        for metric in metrics {
            assert!(
                rows.contains_key(metric) || EXTERNAL.contains(&metric),
                "{failure}: {metric} is not in the reference"
            );
        }
        for alert in alerting {
            assert!(alerts.contains_key(alert), "{failure}: no alert {alert}");
        }
    }
}

#[test]
fn external_series_are_documented() {
    for name in EXTERNAL {
        assert!(
            REFERENCE.contains(&format!("`{name}`")),
            "docs/skys3-metrics.md does not mention {name}"
        );
    }
}
