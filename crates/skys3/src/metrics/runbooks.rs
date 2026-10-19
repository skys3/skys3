//! Checks the operator runbooks (`docs/skys3-runbooks.md`) against the
//! alerting rules, the design's failure matrix and the items that still
//! need a human, the metrics reference, and the tests that drilled them.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use yaml_rust2::YamlLoader;

use super::reference::{ALERTS, DESIGN, REFERENCE, RUNBOOKS as RUNBOOK_URL, cells};

const RUNBOOKS: &str = include_str!("../../../../docs/skys3-runbooks.md");

/// The lines of `document` from the heading `heading` up to the next
/// heading of its level or a higher one.
fn part<'a>(document: &'a str, heading: &str) -> Vec<&'a str> {
    let level = heading.chars().take_while(|c| *c == '#').count();
    let mut lines = document.lines().skip_while(|line| *line != heading);
    let first = lines
        .next()
        .unwrap_or_else(|| panic!("no heading {heading:?}"));
    let rest = lines.take_while(|line| {
        let depth = line.chars().take_while(|c| *c == '#').count();
        !(depth > 0 && depth <= level && line[depth..].starts_with(' '))
    });
    std::iter::once(first).chain(rest).collect()
}

/// The anchor GitHub gives a Markdown heading: lowercase, with spaces as
/// `-`, and without punctuation other than `-` and `_`.
fn slug(heading: &str) -> String {
    heading
        .trim()
        .to_lowercase()
        .chars()
        .filter_map(|c| match c {
            ' ' => Some('-'),
            c if c.is_alphanumeric() || c == '-' || c == '_' => Some(c),
            _ => None,
        })
        .collect()
}

/// The headings of a Markdown document outside code blocks, with their
/// level (the number of `#`).
fn headings(document: &str) -> Vec<(usize, &str)> {
    let mut fenced = false;
    let mut found = Vec::new();
    for line in document.lines() {
        if line.starts_with("```") {
            fenced = !fenced;
            continue;
        }
        let level = line.chars().take_while(|c| *c == '#').count();
        if !fenced && level > 0 && line[level..].starts_with(' ') {
            found.push((level, line[level + 1..].trim()));
        }
    }
    found
}

/// Every anchor of the runbooks, each of them once.
fn anchors() -> BTreeSet<String> {
    let mut anchors = BTreeSet::new();
    for (_, heading) in headings(RUNBOOKS) {
        let anchor = slug(heading);
        assert!(
            anchors.insert(anchor.clone()),
            "two headings have the anchor #{anchor}, so GitHub numbers the second"
        );
    }
    anchors
}

/// The runbooks: the level-3 headings of sections 3 to 6, with their text.
fn runbooks() -> Vec<(&'static str, String)> {
    let mut runbooks = Vec::new();
    for heading in [
        "## 3. Flush and the remote target",
        "## 4. Replication and placement",
        "## 5. Nodes, disks, and clocks",
        "## 6. Control store",
    ] {
        let lines = part(RUNBOOKS, heading);
        let mut current: Option<(&str, Vec<&str>)> = None;
        for line in lines {
            if let Some(name) = line.strip_prefix("### ") {
                runbooks.extend(current.take().map(|(name, body)| (name, body.join("\n"))));
                current = Some((name, Vec::new()));
            } else if let Some((_, body)) = &mut current {
                body.push(line);
            }
        }
        runbooks.extend(current.map(|(name, body)| (name, body.join("\n"))));
    }
    runbooks
}

/// The anchors a Markdown cell links within the document, `](#anchor)`.
fn links(cell: &str) -> Vec<&str> {
    cell.split("](#")
        .skip(1)
        .map(|rest| rest.split(')').next().unwrap())
        .collect()
}

/// The rows of the table in the runbooks' section `heading`, without its
/// header and separator.
fn table(heading: &str) -> Vec<Vec<&'static str>> {
    part(RUNBOOKS, heading)
        .into_iter()
        .filter(|line| line.starts_with("| "))
        .skip(1)
        .map(cells)
        .collect()
}

/// Checks that a table row's runbook links exist.
fn check_links(row: &[&str], anchors: &BTreeSet<String>) {
    let linked = links(row[1]);
    assert!(!linked.is_empty(), "{row:?} links no runbook");
    for anchor in linked {
        assert!(
            anchors.contains(anchor),
            "{row:?} links #{anchor}, which is no heading"
        );
    }
}

#[test]
fn headings_get_github_anchors() {
    assert_eq!(slug("Under-replication"), "under-replication");
    assert_eq!(
        slug("3. Flush and the remote target"),
        "3-flush-and-the-remote-target"
    );
    assert_eq!(slug("Nodes, disks, and clocks"), "nodes-disks-and-clocks");
    assert_eq!(links("[A](#a), [B](#b-c)"), ["a", "b-c"]);
    let document = "# T\n```sh\n# not a heading\n```\n### Sub\n#nope\n";
    assert_eq!(headings(document), [(1, "T"), (3, "Sub")]);
}

#[test]
fn every_alert_links_a_runbook_that_exists() {
    let anchors = anchors();
    let documents = YamlLoader::load_from_str(ALERTS).unwrap();
    let mut alerts = 0;
    for group in documents[0]["groups"].as_vec().unwrap() {
        for rule in group["rules"].as_vec().unwrap() {
            let alert = rule["alert"].as_str().unwrap();
            let url = rule["annotations"]["runbook_url"].as_str().unwrap();
            let anchor = url.strip_prefix(RUNBOOK_URL).unwrap();
            assert!(
                anchors.contains(anchor),
                "{alert} links #{anchor}, which docs/skys3-runbooks.md has no heading for"
            );
            alerts += 1;
        }
    }
    assert!(alerts > 10, "{alerts} alerts");
}

#[test]
fn every_row_of_the_failure_matrix_links_a_runbook() {
    let anchors = anchors();
    let failures: Vec<&str> = part(DESIGN, "## 13. Failure matrix")
        .into_iter()
        .filter(|line| line.starts_with("| ") && !line.starts_with("| Failure"))
        .map(|line| cells(line)[0])
        .collect();
    assert!(failures.len() > 10, "{failures:?}");
    let rows = table("### 2.1 Failure matrix");
    let listed: Vec<&str> = rows.iter().map(|row| row[0]).collect();
    assert_eq!(
        listed, failures,
        "section 2.1 of docs/skys3-runbooks.md has a row for each row of design section 13, in order"
    );
    for row in &rows {
        check_links(row, &anchors);
    }
}

#[test]
fn every_item_that_needs_a_human_links_a_runbook() {
    let anchors = anchors();
    // The bold lead-in of each item of design section 6.9.
    let items: Vec<&str> = part(DESIGN, "### 6.9 What still needs a human")
        .into_iter()
        .filter_map(|line| line.strip_prefix("- **"))
        .map(|line| line.split("**").next().unwrap().trim_end_matches('.'))
        .collect();
    assert_eq!(items.len(), 5, "{items:?}");
    let rows = table("### 2.2 What still needs a human");
    let listed: Vec<&str> = rows.iter().map(|row| row[0]).collect();
    assert_eq!(
        listed, items,
        "section 2.2 of docs/skys3-runbooks.md has a row for each item of design section 6.9, in order"
    );
    for row in &rows {
        check_links(row, &anchors);
    }
}

#[test]
fn every_runbook_has_its_parts_and_a_drill() {
    let runbooks = runbooks();
    assert!(runbooks.len() > 20, "{} runbooks", runbooks.len());
    let drills = table("## 7. Drills");
    let drilled: Vec<&str> = drills.iter().map(|row| row[0]).collect();
    let mut expected = Vec::new();
    for (name, body) in &runbooks {
        // A planned procedure has steps rather than a diagnosis.
        let parts: &[&[&str]] = &[
            &["**Symptoms.**"],
            &["**Impact.**"],
            &["**Diagnosis.**", "**Procedure.**"],
            &["**Remediation.**", "**Procedure.**"],
            &["**Verification.**"],
            &["**Do not.**"],
        ];
        for part in parts {
            assert!(
                part.iter().any(|part| body.contains(part)),
                "runbook {name} has no {}",
                part[0]
            );
        }
        expected.push(format!("[{name}](#{})", slug(name)));
    }
    assert_eq!(
        drilled, expected,
        "section 7 of docs/skys3-runbooks.md has a row for each runbook, in order"
    );
    for row in &drills {
        assert_eq!(row.len(), 4, "{row:?}");
        assert!(row[2..].iter().all(|cell| !cell.is_empty()), "{row:?}");
    }
}

/// The Rust sources under `dir`, without build output.
fn sources(dir: &Path, found: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            if path.file_name().is_some_and(|name| name != "target") {
                sources(&path, found);
            }
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            found.push(path);
        }
    }
}

/// The ends of the paths of the files that may define the test module
/// `module`: a test file such as `runbooks` (`.../runbooks.rs`), or a
/// module of a crate, such as `skys3_coord::policy::tests`
/// (`skys3-coord/src/policy/tests.rs`). A `tests` module may be inline in
/// its parent's file.
fn module_files(module: &str) -> Vec<String> {
    let mut segments: Vec<String> = module.split("::").map(str::to_owned).collect();
    if segments[0].starts_with("skys3_") {
        segments[0] = format!("{}/src", segments[0].replace('_', "-"));
    }
    let mut files = vec![format!("/{}.rs", segments.join("/"))];
    if segments.len() > 1 && segments.last().is_some_and(|last| last == "tests") {
        files.push(format!("/{}.rs", segments[..segments.len() - 1].join("/")));
    }
    files
}

#[test]
fn drill_modules_name_their_files() {
    assert_eq!(module_files("runbooks"), ["/runbooks.rs"]);
    assert_eq!(
        module_files("simulation::takeover"),
        ["/simulation/takeover.rs"]
    );
    assert_eq!(
        module_files("skys3_coord::metrics::tests"),
        [
            "/skys3-coord/src/metrics/tests.rs",
            "/skys3-coord/src/metrics.rs"
        ]
    );
}

#[test]
fn every_drill_names_a_test_that_exists() {
    let crates = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    let mut files = Vec::new();
    sources(&crates, &mut files);
    let mut named = 0;
    for row in table("## 7. Drills") {
        // Test names are in backticks: a module path, then the function.
        for name in row[1].split('`').skip(1).step_by(2) {
            let Some((module, function)) = name.rsplit_once("::") else {
                panic!("{}: drill {name:?} is not <module>::<test>", row[0]);
            };
            let candidates = module_files(module);
            let defined = files.iter().any(|file| {
                let path = file.to_string_lossy();
                candidates.iter().any(|suffix| path.ends_with(suffix))
                    && std::fs::read_to_string(file)
                        .unwrap()
                        .contains(&format!("fn {function}("))
            });
            assert!(
                defined,
                "{}: no test {function} in a file {candidates:?}",
                row[0]
            );
            named += 1;
        }
    }
    assert!(named > 25, "{named} drills");
}

#[test]
fn runbooks_name_only_metrics_the_reference_lists() {
    let mut names = BTreeSet::new();
    let mut rest = RUNBOOKS;
    while let Some(at) = rest.find("skys3_") {
        let name: String = rest[at..]
            .chars()
            .take_while(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == '_')
            .collect();
        rest = &rest[at + name.len()..];
        // A Rust path such as `skys3_flush::snapshot` is not a metric.
        if !rest.starts_with("::") {
            names.insert(name);
        }
    }
    assert!(names.len() > 20, "{names:?}");
    for name in names {
        assert!(
            REFERENCE.contains(&format!("| `{name}` |")),
            "docs/skys3-runbooks.md names {name}, which docs/skys3-metrics.md does not list"
        );
    }
}
