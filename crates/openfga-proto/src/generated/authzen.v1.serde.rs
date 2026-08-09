impl serde::Serialize for Action {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.name.is_empty() {
            len += 1;
        }
        if self.properties.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("authzen.v1.Action", len)?;
        if !self.name.is_empty() {
            struct_ser.serialize_field("name", &self.name)?;
        }
        if let Some(v) = self.properties.as_ref() {
            struct_ser.serialize_field("properties", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Action {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "name",
            "properties",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Name,
            Properties,
            __SkipField__,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "name" => Ok(GeneratedField::Name),
                            "properties" => Ok(GeneratedField::Properties),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Action;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct authzen.v1.Action")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Action, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut name__ = None;
                let mut properties__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Name => {
                            if name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("name"));
                            }
                            name__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Properties => {
                            if properties__.is_some() {
                                return Err(serde::de::Error::duplicate_field("properties"));
                            }
                            properties__ = map_.next_value()?;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(Action {
                    name: name__.unwrap_or_default(),
                    properties: properties__,
                })
            }
        }
        deserializer.deserialize_struct("authzen.v1.Action", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ActionSearchRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.store_id.is_empty() {
            len += 1;
        }
        if self.subject.is_some() {
            len += 1;
        }
        if self.resource.is_some() {
            len += 1;
        }
        if self.context.is_some() {
            len += 1;
        }
        if self.page.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("authzen.v1.ActionSearchRequest", len)?;
        if !self.store_id.is_empty() {
            struct_ser.serialize_field("store_id", &self.store_id)?;
        }
        if let Some(v) = self.subject.as_ref() {
            struct_ser.serialize_field("subject", v)?;
        }
        if let Some(v) = self.resource.as_ref() {
            struct_ser.serialize_field("resource", v)?;
        }
        if let Some(v) = self.context.as_ref() {
            struct_ser.serialize_field("context", v)?;
        }
        if let Some(v) = self.page.as_ref() {
            struct_ser.serialize_field("page", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ActionSearchRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "store_id",
            "subject",
            "resource",
            "context",
            "page",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            StoreId,
            Subject,
            Resource,
            Context,
            Page,
            __SkipField__,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "store_id" => Ok(GeneratedField::StoreId),
                            "subject" => Ok(GeneratedField::Subject),
                            "resource" => Ok(GeneratedField::Resource),
                            "context" => Ok(GeneratedField::Context),
                            "page" => Ok(GeneratedField::Page),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ActionSearchRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct authzen.v1.ActionSearchRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ActionSearchRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut store_id__ = None;
                let mut subject__ = None;
                let mut resource__ = None;
                let mut context__ = None;
                let mut page__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::StoreId => {
                            if store_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("store_id"));
                            }
                            store_id__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Subject => {
                            if subject__.is_some() {
                                return Err(serde::de::Error::duplicate_field("subject"));
                            }
                            subject__ = map_.next_value()?;
                        }
                        GeneratedField::Resource => {
                            if resource__.is_some() {
                                return Err(serde::de::Error::duplicate_field("resource"));
                            }
                            resource__ = map_.next_value()?;
                        }
                        GeneratedField::Context => {
                            if context__.is_some() {
                                return Err(serde::de::Error::duplicate_field("context"));
                            }
                            context__ = map_.next_value()?;
                        }
                        GeneratedField::Page => {
                            if page__.is_some() {
                                return Err(serde::de::Error::duplicate_field("page"));
                            }
                            page__ = map_.next_value()?;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(ActionSearchRequest {
                    store_id: store_id__.unwrap_or_default(),
                    subject: subject__,
                    resource: resource__,
                    context: context__,
                    page: page__,
                })
            }
        }
        deserializer.deserialize_struct("authzen.v1.ActionSearchRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ActionSearchResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.results.is_empty() {
            len += 1;
        }
        if self.page.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("authzen.v1.ActionSearchResponse", len)?;
        if !self.results.is_empty() {
            struct_ser.serialize_field("results", &self.results)?;
        }
        if let Some(v) = self.page.as_ref() {
            struct_ser.serialize_field("page", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ActionSearchResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "results",
            "page",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Results,
            Page,
            __SkipField__,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "results" => Ok(GeneratedField::Results),
                            "page" => Ok(GeneratedField::Page),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ActionSearchResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct authzen.v1.ActionSearchResponse")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ActionSearchResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut results__ = None;
                let mut page__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Results => {
                            if results__.is_some() {
                                return Err(serde::de::Error::duplicate_field("results"));
                            }
                            results__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Page => {
                            if page__.is_some() {
                                return Err(serde::de::Error::duplicate_field("page"));
                            }
                            page__ = map_.next_value()?;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(ActionSearchResponse {
                    results: results__.unwrap_or_default(),
                    page: page__,
                })
            }
        }
        deserializer.deserialize_struct("authzen.v1.ActionSearchResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for EvaluationRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.store_id.is_empty() {
            len += 1;
        }
        if self.subject.is_some() {
            len += 1;
        }
        if self.resource.is_some() {
            len += 1;
        }
        if self.action.is_some() {
            len += 1;
        }
        if self.context.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("authzen.v1.EvaluationRequest", len)?;
        if !self.store_id.is_empty() {
            struct_ser.serialize_field("store_id", &self.store_id)?;
        }
        if let Some(v) = self.subject.as_ref() {
            struct_ser.serialize_field("subject", v)?;
        }
        if let Some(v) = self.resource.as_ref() {
            struct_ser.serialize_field("resource", v)?;
        }
        if let Some(v) = self.action.as_ref() {
            struct_ser.serialize_field("action", v)?;
        }
        if let Some(v) = self.context.as_ref() {
            struct_ser.serialize_field("context", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for EvaluationRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "store_id",
            "subject",
            "resource",
            "action",
            "context",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            StoreId,
            Subject,
            Resource,
            Action,
            Context,
            __SkipField__,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "store_id" => Ok(GeneratedField::StoreId),
                            "subject" => Ok(GeneratedField::Subject),
                            "resource" => Ok(GeneratedField::Resource),
                            "action" => Ok(GeneratedField::Action),
                            "context" => Ok(GeneratedField::Context),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = EvaluationRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct authzen.v1.EvaluationRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<EvaluationRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut store_id__ = None;
                let mut subject__ = None;
                let mut resource__ = None;
                let mut action__ = None;
                let mut context__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::StoreId => {
                            if store_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("store_id"));
                            }
                            store_id__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Subject => {
                            if subject__.is_some() {
                                return Err(serde::de::Error::duplicate_field("subject"));
                            }
                            subject__ = map_.next_value()?;
                        }
                        GeneratedField::Resource => {
                            if resource__.is_some() {
                                return Err(serde::de::Error::duplicate_field("resource"));
                            }
                            resource__ = map_.next_value()?;
                        }
                        GeneratedField::Action => {
                            if action__.is_some() {
                                return Err(serde::de::Error::duplicate_field("action"));
                            }
                            action__ = map_.next_value()?;
                        }
                        GeneratedField::Context => {
                            if context__.is_some() {
                                return Err(serde::de::Error::duplicate_field("context"));
                            }
                            context__ = map_.next_value()?;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(EvaluationRequest {
                    store_id: store_id__.unwrap_or_default(),
                    subject: subject__,
                    resource: resource__,
                    action: action__,
                    context: context__,
                })
            }
        }
        deserializer.deserialize_struct("authzen.v1.EvaluationRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for EvaluationResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 1;
        if self.context.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("authzen.v1.EvaluationResponse", len)?;
        struct_ser.serialize_field("decision", &self.decision)?;
        if let Some(v) = self.context.as_ref() {
            struct_ser.serialize_field("context", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for EvaluationResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "decision",
            "context",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Decision,
            Context,
            __SkipField__,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "decision" => Ok(GeneratedField::Decision),
                            "context" => Ok(GeneratedField::Context),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = EvaluationResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct authzen.v1.EvaluationResponse")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<EvaluationResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut decision__ = None;
                let mut context__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Decision => {
                            if decision__.is_some() {
                                return Err(serde::de::Error::duplicate_field("decision"));
                            }
                            decision__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Context => {
                            if context__.is_some() {
                                return Err(serde::de::Error::duplicate_field("context"));
                            }
                            context__ = map_.next_value()?;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(EvaluationResponse {
                    decision: decision__.unwrap_or_default(),
                    context: context__,
                })
            }
        }
        deserializer.deserialize_struct("authzen.v1.EvaluationResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for EvaluationsItemRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.subject.is_some() {
            len += 1;
        }
        if self.resource.is_some() {
            len += 1;
        }
        if self.action.is_some() {
            len += 1;
        }
        if self.context.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("authzen.v1.EvaluationsItemRequest", len)?;
        if let Some(v) = self.subject.as_ref() {
            struct_ser.serialize_field("subject", v)?;
        }
        if let Some(v) = self.resource.as_ref() {
            struct_ser.serialize_field("resource", v)?;
        }
        if let Some(v) = self.action.as_ref() {
            struct_ser.serialize_field("action", v)?;
        }
        if let Some(v) = self.context.as_ref() {
            struct_ser.serialize_field("context", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for EvaluationsItemRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "subject",
            "resource",
            "action",
            "context",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Subject,
            Resource,
            Action,
            Context,
            __SkipField__,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "subject" => Ok(GeneratedField::Subject),
                            "resource" => Ok(GeneratedField::Resource),
                            "action" => Ok(GeneratedField::Action),
                            "context" => Ok(GeneratedField::Context),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = EvaluationsItemRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct authzen.v1.EvaluationsItemRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<EvaluationsItemRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut subject__ = None;
                let mut resource__ = None;
                let mut action__ = None;
                let mut context__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Subject => {
                            if subject__.is_some() {
                                return Err(serde::de::Error::duplicate_field("subject"));
                            }
                            subject__ = map_.next_value()?;
                        }
                        GeneratedField::Resource => {
                            if resource__.is_some() {
                                return Err(serde::de::Error::duplicate_field("resource"));
                            }
                            resource__ = map_.next_value()?;
                        }
                        GeneratedField::Action => {
                            if action__.is_some() {
                                return Err(serde::de::Error::duplicate_field("action"));
                            }
                            action__ = map_.next_value()?;
                        }
                        GeneratedField::Context => {
                            if context__.is_some() {
                                return Err(serde::de::Error::duplicate_field("context"));
                            }
                            context__ = map_.next_value()?;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(EvaluationsItemRequest {
                    subject: subject__,
                    resource: resource__,
                    action: action__,
                    context: context__,
                })
            }
        }
        deserializer.deserialize_struct("authzen.v1.EvaluationsItemRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for EvaluationsOptions {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.evaluations_semantic != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("authzen.v1.EvaluationsOptions", len)?;
        if self.evaluations_semantic != 0 {
            let v = EvaluationsSemantic::try_from(self.evaluations_semantic)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.evaluations_semantic)))?;
            struct_ser.serialize_field("evaluations_semantic", &v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for EvaluationsOptions {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "evaluations_semantic",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            EvaluationsSemantic,
            __SkipField__,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "evaluations_semantic" => Ok(GeneratedField::EvaluationsSemantic),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = EvaluationsOptions;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct authzen.v1.EvaluationsOptions")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<EvaluationsOptions, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut evaluations_semantic__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::EvaluationsSemantic => {
                            if evaluations_semantic__.is_some() {
                                return Err(serde::de::Error::duplicate_field("evaluations_semantic"));
                            }
                            evaluations_semantic__ = Some(map_.next_value::<EvaluationsSemantic>()? as i32);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(EvaluationsOptions {
                    evaluations_semantic: evaluations_semantic__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("authzen.v1.EvaluationsOptions", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for EvaluationsRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.store_id.is_empty() {
            len += 1;
        }
        if self.subject.is_some() {
            len += 1;
        }
        if self.action.is_some() {
            len += 1;
        }
        if self.resource.is_some() {
            len += 1;
        }
        if self.context.is_some() {
            len += 1;
        }
        if !self.evaluations.is_empty() {
            len += 1;
        }
        if self.options.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("authzen.v1.EvaluationsRequest", len)?;
        if !self.store_id.is_empty() {
            struct_ser.serialize_field("store_id", &self.store_id)?;
        }
        if let Some(v) = self.subject.as_ref() {
            struct_ser.serialize_field("subject", v)?;
        }
        if let Some(v) = self.action.as_ref() {
            struct_ser.serialize_field("action", v)?;
        }
        if let Some(v) = self.resource.as_ref() {
            struct_ser.serialize_field("resource", v)?;
        }
        if let Some(v) = self.context.as_ref() {
            struct_ser.serialize_field("context", v)?;
        }
        if !self.evaluations.is_empty() {
            struct_ser.serialize_field("evaluations", &self.evaluations)?;
        }
        if let Some(v) = self.options.as_ref() {
            struct_ser.serialize_field("options", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for EvaluationsRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "store_id",
            "subject",
            "action",
            "resource",
            "context",
            "evaluations",
            "options",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            StoreId,
            Subject,
            Action,
            Resource,
            Context,
            Evaluations,
            Options,
            __SkipField__,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "store_id" => Ok(GeneratedField::StoreId),
                            "subject" => Ok(GeneratedField::Subject),
                            "action" => Ok(GeneratedField::Action),
                            "resource" => Ok(GeneratedField::Resource),
                            "context" => Ok(GeneratedField::Context),
                            "evaluations" => Ok(GeneratedField::Evaluations),
                            "options" => Ok(GeneratedField::Options),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = EvaluationsRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct authzen.v1.EvaluationsRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<EvaluationsRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut store_id__ = None;
                let mut subject__ = None;
                let mut action__ = None;
                let mut resource__ = None;
                let mut context__ = None;
                let mut evaluations__ = None;
                let mut options__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::StoreId => {
                            if store_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("store_id"));
                            }
                            store_id__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Subject => {
                            if subject__.is_some() {
                                return Err(serde::de::Error::duplicate_field("subject"));
                            }
                            subject__ = map_.next_value()?;
                        }
                        GeneratedField::Action => {
                            if action__.is_some() {
                                return Err(serde::de::Error::duplicate_field("action"));
                            }
                            action__ = map_.next_value()?;
                        }
                        GeneratedField::Resource => {
                            if resource__.is_some() {
                                return Err(serde::de::Error::duplicate_field("resource"));
                            }
                            resource__ = map_.next_value()?;
                        }
                        GeneratedField::Context => {
                            if context__.is_some() {
                                return Err(serde::de::Error::duplicate_field("context"));
                            }
                            context__ = map_.next_value()?;
                        }
                        GeneratedField::Evaluations => {
                            if evaluations__.is_some() {
                                return Err(serde::de::Error::duplicate_field("evaluations"));
                            }
                            evaluations__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Options => {
                            if options__.is_some() {
                                return Err(serde::de::Error::duplicate_field("options"));
                            }
                            options__ = map_.next_value()?;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(EvaluationsRequest {
                    store_id: store_id__.unwrap_or_default(),
                    subject: subject__,
                    action: action__,
                    resource: resource__,
                    context: context__,
                    evaluations: evaluations__.unwrap_or_default(),
                    options: options__,
                })
            }
        }
        deserializer.deserialize_struct("authzen.v1.EvaluationsRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for EvaluationsResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.evaluations.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("authzen.v1.EvaluationsResponse", len)?;
        if !self.evaluations.is_empty() {
            struct_ser.serialize_field("evaluations", &self.evaluations)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for EvaluationsResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "evaluations",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Evaluations,
            __SkipField__,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "evaluations" => Ok(GeneratedField::Evaluations),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = EvaluationsResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct authzen.v1.EvaluationsResponse")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<EvaluationsResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut evaluations__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Evaluations => {
                            if evaluations__.is_some() {
                                return Err(serde::de::Error::duplicate_field("evaluations"));
                            }
                            evaluations__ = Some(map_.next_value()?);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(EvaluationsResponse {
                    evaluations: evaluations__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("authzen.v1.EvaluationsResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for EvaluationsSemantic {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::ExecuteAll => "execute_all",
            Self::DenyOnFirstDeny => "deny_on_first_deny",
            Self::PermitOnFirstPermit => "permit_on_first_permit",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for EvaluationsSemantic {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "execute_all",
            "deny_on_first_deny",
            "permit_on_first_permit",
        ];

        struct GeneratedVisitor;

        impl serde::de::Visitor<'_> for GeneratedVisitor {
            type Value = EvaluationsSemantic;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "execute_all" => Ok(EvaluationsSemantic::ExecuteAll),
                    "deny_on_first_deny" => Ok(EvaluationsSemantic::DenyOnFirstDeny),
                    "permit_on_first_permit" => Ok(EvaluationsSemantic::PermitOnFirstPermit),
                    _ => Ok(EvaluationsSemantic::default()),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for GetConfigurationRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.store_id.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("authzen.v1.GetConfigurationRequest", len)?;
        if !self.store_id.is_empty() {
            struct_ser.serialize_field("store_id", &self.store_id)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for GetConfigurationRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "store_id",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            StoreId,
            __SkipField__,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "store_id" => Ok(GeneratedField::StoreId),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = GetConfigurationRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct authzen.v1.GetConfigurationRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<GetConfigurationRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut store_id__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::StoreId => {
                            if store_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("store_id"));
                            }
                            store_id__ = Some(map_.next_value()?);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(GetConfigurationRequest {
                    store_id: store_id__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("authzen.v1.GetConfigurationRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for GetConfigurationResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.policy_decision_point.is_empty() {
            len += 1;
        }
        if !self.access_evaluation_endpoint.is_empty() {
            len += 1;
        }
        if !self.access_evaluations_endpoint.is_empty() {
            len += 1;
        }
        if !self.search_subject_endpoint.is_empty() {
            len += 1;
        }
        if !self.search_resource_endpoint.is_empty() {
            len += 1;
        }
        if !self.search_action_endpoint.is_empty() {
            len += 1;
        }
        if !self.capabilities.is_empty() {
            len += 1;
        }
        if self.signed_metadata.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("authzen.v1.GetConfigurationResponse", len)?;
        if !self.policy_decision_point.is_empty() {
            struct_ser.serialize_field("policy_decision_point", &self.policy_decision_point)?;
        }
        if !self.access_evaluation_endpoint.is_empty() {
            struct_ser.serialize_field("access_evaluation_endpoint", &self.access_evaluation_endpoint)?;
        }
        if !self.access_evaluations_endpoint.is_empty() {
            struct_ser.serialize_field("access_evaluations_endpoint", &self.access_evaluations_endpoint)?;
        }
        if !self.search_subject_endpoint.is_empty() {
            struct_ser.serialize_field("search_subject_endpoint", &self.search_subject_endpoint)?;
        }
        if !self.search_resource_endpoint.is_empty() {
            struct_ser.serialize_field("search_resource_endpoint", &self.search_resource_endpoint)?;
        }
        if !self.search_action_endpoint.is_empty() {
            struct_ser.serialize_field("search_action_endpoint", &self.search_action_endpoint)?;
        }
        if !self.capabilities.is_empty() {
            struct_ser.serialize_field("capabilities", &self.capabilities)?;
        }
        if let Some(v) = self.signed_metadata.as_ref() {
            struct_ser.serialize_field("signed_metadata", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for GetConfigurationResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "policy_decision_point",
            "access_evaluation_endpoint",
            "access_evaluations_endpoint",
            "search_subject_endpoint",
            "search_resource_endpoint",
            "search_action_endpoint",
            "capabilities",
            "signed_metadata",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            PolicyDecisionPoint,
            AccessEvaluationEndpoint,
            AccessEvaluationsEndpoint,
            SearchSubjectEndpoint,
            SearchResourceEndpoint,
            SearchActionEndpoint,
            Capabilities,
            SignedMetadata,
            __SkipField__,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "policy_decision_point" => Ok(GeneratedField::PolicyDecisionPoint),
                            "access_evaluation_endpoint" => Ok(GeneratedField::AccessEvaluationEndpoint),
                            "access_evaluations_endpoint" => Ok(GeneratedField::AccessEvaluationsEndpoint),
                            "search_subject_endpoint" => Ok(GeneratedField::SearchSubjectEndpoint),
                            "search_resource_endpoint" => Ok(GeneratedField::SearchResourceEndpoint),
                            "search_action_endpoint" => Ok(GeneratedField::SearchActionEndpoint),
                            "capabilities" => Ok(GeneratedField::Capabilities),
                            "signed_metadata" => Ok(GeneratedField::SignedMetadata),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = GetConfigurationResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct authzen.v1.GetConfigurationResponse")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<GetConfigurationResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut policy_decision_point__ = None;
                let mut access_evaluation_endpoint__ = None;
                let mut access_evaluations_endpoint__ = None;
                let mut search_subject_endpoint__ = None;
                let mut search_resource_endpoint__ = None;
                let mut search_action_endpoint__ = None;
                let mut capabilities__ = None;
                let mut signed_metadata__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::PolicyDecisionPoint => {
                            if policy_decision_point__.is_some() {
                                return Err(serde::de::Error::duplicate_field("policy_decision_point"));
                            }
                            policy_decision_point__ = Some(map_.next_value()?);
                        }
                        GeneratedField::AccessEvaluationEndpoint => {
                            if access_evaluation_endpoint__.is_some() {
                                return Err(serde::de::Error::duplicate_field("access_evaluation_endpoint"));
                            }
                            access_evaluation_endpoint__ = Some(map_.next_value()?);
                        }
                        GeneratedField::AccessEvaluationsEndpoint => {
                            if access_evaluations_endpoint__.is_some() {
                                return Err(serde::de::Error::duplicate_field("access_evaluations_endpoint"));
                            }
                            access_evaluations_endpoint__ = Some(map_.next_value()?);
                        }
                        GeneratedField::SearchSubjectEndpoint => {
                            if search_subject_endpoint__.is_some() {
                                return Err(serde::de::Error::duplicate_field("search_subject_endpoint"));
                            }
                            search_subject_endpoint__ = Some(map_.next_value()?);
                        }
                        GeneratedField::SearchResourceEndpoint => {
                            if search_resource_endpoint__.is_some() {
                                return Err(serde::de::Error::duplicate_field("search_resource_endpoint"));
                            }
                            search_resource_endpoint__ = Some(map_.next_value()?);
                        }
                        GeneratedField::SearchActionEndpoint => {
                            if search_action_endpoint__.is_some() {
                                return Err(serde::de::Error::duplicate_field("search_action_endpoint"));
                            }
                            search_action_endpoint__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Capabilities => {
                            if capabilities__.is_some() {
                                return Err(serde::de::Error::duplicate_field("capabilities"));
                            }
                            capabilities__ = Some(map_.next_value()?);
                        }
                        GeneratedField::SignedMetadata => {
                            if signed_metadata__.is_some() {
                                return Err(serde::de::Error::duplicate_field("signed_metadata"));
                            }
                            signed_metadata__ = map_.next_value()?;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(GetConfigurationResponse {
                    policy_decision_point: policy_decision_point__.unwrap_or_default(),
                    access_evaluation_endpoint: access_evaluation_endpoint__.unwrap_or_default(),
                    access_evaluations_endpoint: access_evaluations_endpoint__.unwrap_or_default(),
                    search_subject_endpoint: search_subject_endpoint__.unwrap_or_default(),
                    search_resource_endpoint: search_resource_endpoint__.unwrap_or_default(),
                    search_action_endpoint: search_action_endpoint__.unwrap_or_default(),
                    capabilities: capabilities__.unwrap_or_default(),
                    signed_metadata: signed_metadata__,
                })
            }
        }
        deserializer.deserialize_struct("authzen.v1.GetConfigurationResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PageRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.token.is_some() {
            len += 1;
        }
        if self.limit.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("authzen.v1.PageRequest", len)?;
        if let Some(v) = self.token.as_ref() {
            struct_ser.serialize_field("token", v)?;
        }
        if let Some(v) = self.limit.as_ref() {
            struct_ser.serialize_field("limit", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PageRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "token",
            "limit",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Token,
            Limit,
            __SkipField__,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "token" => Ok(GeneratedField::Token),
                            "limit" => Ok(GeneratedField::Limit),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PageRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct authzen.v1.PageRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<PageRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut token__ = None;
                let mut limit__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Token => {
                            if token__.is_some() {
                                return Err(serde::de::Error::duplicate_field("token"));
                            }
                            token__ = map_.next_value()?;
                        }
                        GeneratedField::Limit => {
                            if limit__.is_some() {
                                return Err(serde::de::Error::duplicate_field("limit"));
                            }
                            limit__ = 
                                map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| x.0)
                            ;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(PageRequest {
                    token: token__,
                    limit: limit__,
                })
            }
        }
        deserializer.deserialize_struct("authzen.v1.PageRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PageResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.next_token.is_empty() {
            len += 1;
        }
        if self.count != 0 {
            len += 1;
        }
        if self.total.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("authzen.v1.PageResponse", len)?;
        if !self.next_token.is_empty() {
            struct_ser.serialize_field("next_token", &self.next_token)?;
        }
        if self.count != 0 {
            struct_ser.serialize_field("count", &self.count)?;
        }
        if let Some(v) = self.total.as_ref() {
            struct_ser.serialize_field("total", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PageResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "next_token",
            "count",
            "total",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            NextToken,
            Count,
            Total,
            __SkipField__,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "next_token" => Ok(GeneratedField::NextToken),
                            "count" => Ok(GeneratedField::Count),
                            "total" => Ok(GeneratedField::Total),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PageResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct authzen.v1.PageResponse")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<PageResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut next_token__ = None;
                let mut count__ = None;
                let mut total__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::NextToken => {
                            if next_token__.is_some() {
                                return Err(serde::de::Error::duplicate_field("next_token"));
                            }
                            next_token__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Count => {
                            if count__.is_some() {
                                return Err(serde::de::Error::duplicate_field("count"));
                            }
                            count__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Total => {
                            if total__.is_some() {
                                return Err(serde::de::Error::duplicate_field("total"));
                            }
                            total__ = 
                                map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| x.0)
                            ;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(PageResponse {
                    next_token: next_token__.unwrap_or_default(),
                    count: count__.unwrap_or_default(),
                    total: total__,
                })
            }
        }
        deserializer.deserialize_struct("authzen.v1.PageResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Resource {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.r#type.is_empty() {
            len += 1;
        }
        if !self.id.is_empty() {
            len += 1;
        }
        if self.properties.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("authzen.v1.Resource", len)?;
        if !self.r#type.is_empty() {
            struct_ser.serialize_field("type", &self.r#type)?;
        }
        if !self.id.is_empty() {
            struct_ser.serialize_field("id", &self.id)?;
        }
        if let Some(v) = self.properties.as_ref() {
            struct_ser.serialize_field("properties", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Resource {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "type",
            "id",
            "properties",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Type,
            Id,
            Properties,
            __SkipField__,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "type" => Ok(GeneratedField::Type),
                            "id" => Ok(GeneratedField::Id),
                            "properties" => Ok(GeneratedField::Properties),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Resource;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct authzen.v1.Resource")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Resource, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut r#type__ = None;
                let mut id__ = None;
                let mut properties__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Type => {
                            if r#type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("type"));
                            }
                            r#type__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Id => {
                            if id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("id"));
                            }
                            id__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Properties => {
                            if properties__.is_some() {
                                return Err(serde::de::Error::duplicate_field("properties"));
                            }
                            properties__ = map_.next_value()?;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(Resource {
                    r#type: r#type__.unwrap_or_default(),
                    id: id__.unwrap_or_default(),
                    properties: properties__,
                })
            }
        }
        deserializer.deserialize_struct("authzen.v1.Resource", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ResourceFilter {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.r#type.is_empty() {
            len += 1;
        }
        if self.id.is_some() {
            len += 1;
        }
        if self.properties.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("authzen.v1.ResourceFilter", len)?;
        if !self.r#type.is_empty() {
            struct_ser.serialize_field("type", &self.r#type)?;
        }
        if let Some(v) = self.id.as_ref() {
            struct_ser.serialize_field("id", v)?;
        }
        if let Some(v) = self.properties.as_ref() {
            struct_ser.serialize_field("properties", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ResourceFilter {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "type",
            "id",
            "properties",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Type,
            Id,
            Properties,
            __SkipField__,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "type" => Ok(GeneratedField::Type),
                            "id" => Ok(GeneratedField::Id),
                            "properties" => Ok(GeneratedField::Properties),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ResourceFilter;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct authzen.v1.ResourceFilter")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ResourceFilter, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut r#type__ = None;
                let mut id__ = None;
                let mut properties__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Type => {
                            if r#type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("type"));
                            }
                            r#type__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Id => {
                            if id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("id"));
                            }
                            id__ = map_.next_value()?;
                        }
                        GeneratedField::Properties => {
                            if properties__.is_some() {
                                return Err(serde::de::Error::duplicate_field("properties"));
                            }
                            properties__ = map_.next_value()?;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(ResourceFilter {
                    r#type: r#type__.unwrap_or_default(),
                    id: id__,
                    properties: properties__,
                })
            }
        }
        deserializer.deserialize_struct("authzen.v1.ResourceFilter", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ResourceSearchRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.store_id.is_empty() {
            len += 1;
        }
        if self.subject.is_some() {
            len += 1;
        }
        if self.action.is_some() {
            len += 1;
        }
        if self.resource.is_some() {
            len += 1;
        }
        if self.context.is_some() {
            len += 1;
        }
        if self.page.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("authzen.v1.ResourceSearchRequest", len)?;
        if !self.store_id.is_empty() {
            struct_ser.serialize_field("store_id", &self.store_id)?;
        }
        if let Some(v) = self.subject.as_ref() {
            struct_ser.serialize_field("subject", v)?;
        }
        if let Some(v) = self.action.as_ref() {
            struct_ser.serialize_field("action", v)?;
        }
        if let Some(v) = self.resource.as_ref() {
            struct_ser.serialize_field("resource", v)?;
        }
        if let Some(v) = self.context.as_ref() {
            struct_ser.serialize_field("context", v)?;
        }
        if let Some(v) = self.page.as_ref() {
            struct_ser.serialize_field("page", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ResourceSearchRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "store_id",
            "subject",
            "action",
            "resource",
            "context",
            "page",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            StoreId,
            Subject,
            Action,
            Resource,
            Context,
            Page,
            __SkipField__,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "store_id" => Ok(GeneratedField::StoreId),
                            "subject" => Ok(GeneratedField::Subject),
                            "action" => Ok(GeneratedField::Action),
                            "resource" => Ok(GeneratedField::Resource),
                            "context" => Ok(GeneratedField::Context),
                            "page" => Ok(GeneratedField::Page),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ResourceSearchRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct authzen.v1.ResourceSearchRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ResourceSearchRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut store_id__ = None;
                let mut subject__ = None;
                let mut action__ = None;
                let mut resource__ = None;
                let mut context__ = None;
                let mut page__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::StoreId => {
                            if store_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("store_id"));
                            }
                            store_id__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Subject => {
                            if subject__.is_some() {
                                return Err(serde::de::Error::duplicate_field("subject"));
                            }
                            subject__ = map_.next_value()?;
                        }
                        GeneratedField::Action => {
                            if action__.is_some() {
                                return Err(serde::de::Error::duplicate_field("action"));
                            }
                            action__ = map_.next_value()?;
                        }
                        GeneratedField::Resource => {
                            if resource__.is_some() {
                                return Err(serde::de::Error::duplicate_field("resource"));
                            }
                            resource__ = map_.next_value()?;
                        }
                        GeneratedField::Context => {
                            if context__.is_some() {
                                return Err(serde::de::Error::duplicate_field("context"));
                            }
                            context__ = map_.next_value()?;
                        }
                        GeneratedField::Page => {
                            if page__.is_some() {
                                return Err(serde::de::Error::duplicate_field("page"));
                            }
                            page__ = map_.next_value()?;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(ResourceSearchRequest {
                    store_id: store_id__.unwrap_or_default(),
                    subject: subject__,
                    action: action__,
                    resource: resource__,
                    context: context__,
                    page: page__,
                })
            }
        }
        deserializer.deserialize_struct("authzen.v1.ResourceSearchRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ResourceSearchResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.results.is_empty() {
            len += 1;
        }
        if self.page.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("authzen.v1.ResourceSearchResponse", len)?;
        if !self.results.is_empty() {
            struct_ser.serialize_field("results", &self.results)?;
        }
        if let Some(v) = self.page.as_ref() {
            struct_ser.serialize_field("page", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ResourceSearchResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "results",
            "page",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Results,
            Page,
            __SkipField__,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "results" => Ok(GeneratedField::Results),
                            "page" => Ok(GeneratedField::Page),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ResourceSearchResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct authzen.v1.ResourceSearchResponse")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ResourceSearchResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut results__ = None;
                let mut page__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Results => {
                            if results__.is_some() {
                                return Err(serde::de::Error::duplicate_field("results"));
                            }
                            results__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Page => {
                            if page__.is_some() {
                                return Err(serde::de::Error::duplicate_field("page"));
                            }
                            page__ = map_.next_value()?;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(ResourceSearchResponse {
                    results: results__.unwrap_or_default(),
                    page: page__,
                })
            }
        }
        deserializer.deserialize_struct("authzen.v1.ResourceSearchResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Subject {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.r#type.is_empty() {
            len += 1;
        }
        if !self.id.is_empty() {
            len += 1;
        }
        if self.properties.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("authzen.v1.Subject", len)?;
        if !self.r#type.is_empty() {
            struct_ser.serialize_field("type", &self.r#type)?;
        }
        if !self.id.is_empty() {
            struct_ser.serialize_field("id", &self.id)?;
        }
        if let Some(v) = self.properties.as_ref() {
            struct_ser.serialize_field("properties", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Subject {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "type",
            "id",
            "properties",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Type,
            Id,
            Properties,
            __SkipField__,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "type" => Ok(GeneratedField::Type),
                            "id" => Ok(GeneratedField::Id),
                            "properties" => Ok(GeneratedField::Properties),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Subject;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct authzen.v1.Subject")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Subject, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut r#type__ = None;
                let mut id__ = None;
                let mut properties__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Type => {
                            if r#type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("type"));
                            }
                            r#type__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Id => {
                            if id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("id"));
                            }
                            id__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Properties => {
                            if properties__.is_some() {
                                return Err(serde::de::Error::duplicate_field("properties"));
                            }
                            properties__ = map_.next_value()?;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(Subject {
                    r#type: r#type__.unwrap_or_default(),
                    id: id__.unwrap_or_default(),
                    properties: properties__,
                })
            }
        }
        deserializer.deserialize_struct("authzen.v1.Subject", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SubjectFilter {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.r#type.is_empty() {
            len += 1;
        }
        if self.id.is_some() {
            len += 1;
        }
        if self.properties.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("authzen.v1.SubjectFilter", len)?;
        if !self.r#type.is_empty() {
            struct_ser.serialize_field("type", &self.r#type)?;
        }
        if let Some(v) = self.id.as_ref() {
            struct_ser.serialize_field("id", v)?;
        }
        if let Some(v) = self.properties.as_ref() {
            struct_ser.serialize_field("properties", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SubjectFilter {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "type",
            "id",
            "properties",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Type,
            Id,
            Properties,
            __SkipField__,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "type" => Ok(GeneratedField::Type),
                            "id" => Ok(GeneratedField::Id),
                            "properties" => Ok(GeneratedField::Properties),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SubjectFilter;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct authzen.v1.SubjectFilter")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<SubjectFilter, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut r#type__ = None;
                let mut id__ = None;
                let mut properties__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Type => {
                            if r#type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("type"));
                            }
                            r#type__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Id => {
                            if id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("id"));
                            }
                            id__ = map_.next_value()?;
                        }
                        GeneratedField::Properties => {
                            if properties__.is_some() {
                                return Err(serde::de::Error::duplicate_field("properties"));
                            }
                            properties__ = map_.next_value()?;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(SubjectFilter {
                    r#type: r#type__.unwrap_or_default(),
                    id: id__,
                    properties: properties__,
                })
            }
        }
        deserializer.deserialize_struct("authzen.v1.SubjectFilter", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SubjectSearchRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.store_id.is_empty() {
            len += 1;
        }
        if self.resource.is_some() {
            len += 1;
        }
        if self.action.is_some() {
            len += 1;
        }
        if self.subject.is_some() {
            len += 1;
        }
        if self.context.is_some() {
            len += 1;
        }
        if self.page.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("authzen.v1.SubjectSearchRequest", len)?;
        if !self.store_id.is_empty() {
            struct_ser.serialize_field("store_id", &self.store_id)?;
        }
        if let Some(v) = self.resource.as_ref() {
            struct_ser.serialize_field("resource", v)?;
        }
        if let Some(v) = self.action.as_ref() {
            struct_ser.serialize_field("action", v)?;
        }
        if let Some(v) = self.subject.as_ref() {
            struct_ser.serialize_field("subject", v)?;
        }
        if let Some(v) = self.context.as_ref() {
            struct_ser.serialize_field("context", v)?;
        }
        if let Some(v) = self.page.as_ref() {
            struct_ser.serialize_field("page", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SubjectSearchRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "store_id",
            "resource",
            "action",
            "subject",
            "context",
            "page",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            StoreId,
            Resource,
            Action,
            Subject,
            Context,
            Page,
            __SkipField__,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "store_id" => Ok(GeneratedField::StoreId),
                            "resource" => Ok(GeneratedField::Resource),
                            "action" => Ok(GeneratedField::Action),
                            "subject" => Ok(GeneratedField::Subject),
                            "context" => Ok(GeneratedField::Context),
                            "page" => Ok(GeneratedField::Page),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SubjectSearchRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct authzen.v1.SubjectSearchRequest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<SubjectSearchRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut store_id__ = None;
                let mut resource__ = None;
                let mut action__ = None;
                let mut subject__ = None;
                let mut context__ = None;
                let mut page__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::StoreId => {
                            if store_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("store_id"));
                            }
                            store_id__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Resource => {
                            if resource__.is_some() {
                                return Err(serde::de::Error::duplicate_field("resource"));
                            }
                            resource__ = map_.next_value()?;
                        }
                        GeneratedField::Action => {
                            if action__.is_some() {
                                return Err(serde::de::Error::duplicate_field("action"));
                            }
                            action__ = map_.next_value()?;
                        }
                        GeneratedField::Subject => {
                            if subject__.is_some() {
                                return Err(serde::de::Error::duplicate_field("subject"));
                            }
                            subject__ = map_.next_value()?;
                        }
                        GeneratedField::Context => {
                            if context__.is_some() {
                                return Err(serde::de::Error::duplicate_field("context"));
                            }
                            context__ = map_.next_value()?;
                        }
                        GeneratedField::Page => {
                            if page__.is_some() {
                                return Err(serde::de::Error::duplicate_field("page"));
                            }
                            page__ = map_.next_value()?;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(SubjectSearchRequest {
                    store_id: store_id__.unwrap_or_default(),
                    resource: resource__,
                    action: action__,
                    subject: subject__,
                    context: context__,
                    page: page__,
                })
            }
        }
        deserializer.deserialize_struct("authzen.v1.SubjectSearchRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SubjectSearchResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.results.is_empty() {
            len += 1;
        }
        if self.page.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("authzen.v1.SubjectSearchResponse", len)?;
        if !self.results.is_empty() {
            struct_ser.serialize_field("results", &self.results)?;
        }
        if let Some(v) = self.page.as_ref() {
            struct_ser.serialize_field("page", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SubjectSearchResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "results",
            "page",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Results,
            Page,
            __SkipField__,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "results" => Ok(GeneratedField::Results),
                            "page" => Ok(GeneratedField::Page),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SubjectSearchResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct authzen.v1.SubjectSearchResponse")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<SubjectSearchResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut results__ = None;
                let mut page__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Results => {
                            if results__.is_some() {
                                return Err(serde::de::Error::duplicate_field("results"));
                            }
                            results__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Page => {
                            if page__.is_some() {
                                return Err(serde::de::Error::duplicate_field("page"));
                            }
                            page__ = map_.next_value()?;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(SubjectSearchResponse {
                    results: results__.unwrap_or_default(),
                    page: page__,
                })
            }
        }
        deserializer.deserialize_struct("authzen.v1.SubjectSearchResponse", FIELDS, GeneratedVisitor)
    }
}
