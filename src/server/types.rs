use std::time::Instant;

pub(crate) struct Value {
    /// The actual value
    value: String,

    /// Last set time (if key was set with expirations)
    expiration: Option<Instant>,
}

impl Value {
    pub(crate) fn new(value: String, expiration: Option<Instant>) -> Self {
        Self { value, expiration }
    }

    pub(crate) fn expired(&self, current: Instant) -> bool {
        if let Some(expiration) = self.expiration {
            if current >= expiration {
                // key is now expired
                true
            } else {
                // key still valid
                false
            }
        } else {
            // Not expired because it can't
            false
        }
    }

    pub(crate) fn get_value(&self) -> String {
        self.value.clone()
    }
}
