//! Just enough XML: writing S3 responses and reading the few small request
//! bodies (CompleteMultipartUpload, Delete, Tagging).

pub const NS: &str = "http://s3.amazonaws.com/doc/2006-03-01/";

pub fn escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            c => out.push(c),
        }
    }
    out
}

pub fn unescape(s: &str) -> String {
    s.replace("&lt;", "<").replace("&gt;", ">").replace("&quot;", "\"").replace("&apos;", "'").replace("&amp;", "&")
}

#[derive(Default)]
pub struct Xml(String);

impl Xml {
    pub fn open(&mut self, tag: &str) -> &mut Self {
        self.0.push('<');
        self.0.push_str(tag);
        self.0.push('>');
        self
    }

    pub fn close(&mut self, tag: &str) -> &mut Self {
        self.0.push_str("</");
        self.0.push_str(tag);
        self.0.push('>');
        self
    }

    /// Already escaped markup or text.
    pub fn raw(&mut self, markup: &str) -> &mut Self {
        self.0.push_str(markup);
        self
    }

    pub fn leaf(&mut self, tag: &str, text: &str) -> &mut Self {
        self.open(tag);
        self.0.push_str(&escape(text));
        self.close(tag)
    }
}

/// `<?xml …?><root xmlns="…">…</root>`.
pub fn document(root: &str, build: impl FnOnce(&mut Xml)) -> String {
    let mut x = Xml(format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<{root} xmlns=\"{NS}\">"));
    build(&mut x);
    x.close(root);
    x.0
}

/// Inner text of every `<name>` / `<name …>` element (no nesting of the same name).
pub fn elements<'a>(xml: &'a str, name: &str) -> Vec<&'a str> {
    let mut out = Vec::new();
    let close = format!("</{name}>");
    let mut rest = xml;
    while let Some(start) = rest.find(&format!("<{name}")) {
        let after = &rest[start + name.len() + 1..];
        let Some(gt) = after.find('>') else { break };
        let head = &after[..gt];
        if !(head.is_empty() || head.starts_with(char::is_whitespace)) {
            rest = &after[gt..];
            continue;
        }
        if head.ends_with('/') {
            out.push("");
            rest = &after[gt + 1..];
            continue;
        }
        let body = &after[gt + 1..];
        let Some(end) = body.find(&close) else { break };
        out.push(&body[..end]);
        rest = &body[end + close.len()..];
    }
    out
}

pub fn child(xml: &str, name: &str) -> Option<String> {
    elements(xml, name).first().map(|v| unescape(v.trim()))
}

/// S3 timestamps: `2026-10-26T12:00:00.000Z`.
pub fn iso8601(ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ms).unwrap_or_default().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_and_write() {
        let body = r#"<?xml version="1.0"?><CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Part><PartNumber>1</PartNumber><ETag>"a&amp;b"</ETag></Part>
            <Part><PartNumber>2</PartNumber><ETag>"c"</ETag></Part></CompleteMultipartUpload>"#;
        let parts = elements(body, "Part");
        assert_eq!(parts.len(), 2);
        assert_eq!(child(parts[0], "ETag").as_deref(), Some("\"a&b\""));
        assert_eq!(child(parts[1], "PartNumber").as_deref(), Some("2"));
        assert!(elements("<Partial>x</Partial>", "Part").is_empty());
        let doc = document("Tagging", |x| {
            x.open("TagSet").open("Tag").leaf("Key", "a<b").leaf("Value", "v").close("Tag").close("TagSet");
        });
        assert!(doc.ends_with("<Tagging xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><TagSet><Tag><Key>a&lt;b</Key><Value>v</Value></Tag></TagSet></Tagging>"));
        assert_eq!(iso8601(0), "1970-01-01T00:00:00.000Z");
    }
}
