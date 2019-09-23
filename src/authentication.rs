pub enum Authentication<'a> {
    Tls {
        certificate_path: &'a str,
        private_key_path: &'a str,
    },
    Token(&'a str),
    Athenz(&'a str),
}
