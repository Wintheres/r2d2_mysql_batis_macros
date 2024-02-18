use proc_macro::TokenStream;
use std::ops::Add;
use quote::quote;
use syn::{parse_macro_input, DeriveInput, Fields, Type};

#[proc_macro_attribute]
pub fn entity_option_mapping(attr: TokenStream, input: TokenStream) -> TokenStream {
    let attr = attr.to_string();
    let mut attr_split = attr.split(",");
    let mut table_name = "";
    if let Some(table_name_option) = attr_split.next() {
        table_name = table_name_option.trim();
    }
    let mut primary_key = "";
    if let Some(primary_key_option) = attr_split.next() {
        primary_key = primary_key_option.trim();
    }
    let input_tokens: proc_macro2::TokenStream = input.clone().into();
    let derive_input = parse_macro_input!(input as DeriveInput);
    let struct_name = &derive_input.ident;
    let mut primary_key_name = &derive_input.ident;

    if let syn::Data::Struct(data) = &derive_input.data {
        if let Fields::Named(named_fields) = &data.fields {
            let mut assignment = vec![];
            let mut stmts = vec![];
            let mut params = vec![];
            let mut update_sql = vec![];
            let mut type_names = vec![];

            for field in &named_fields.named {
                if let Some(ident) = &field.ident {
                    assignment.push(quote! {#ident: None,});
                    let field_name = ident.to_string();
                    if field_name.eq(primary_key) {
                        primary_key_name = &ident;
                    }
                    type_names.push(field_name.clone());
                    update_sql.push(quote!{
                        if !self.#ident.eq(&None) {
                            update_sql = update_sql.add(" `");
                            update_sql = update_sql.add(#field_name);
                            update_sql = update_sql.add("` = :");
                            update_sql = update_sql.add(#field_name);
                            update_sql = update_sql.add(",");
                        }
                    });
                    let field_type = &field.ty;
                    if let Type::Path(type_path) = field_type {
                        // let type_name = &type_path.path.segments.last().unwrap().ident;
                        // let type_name = type_name.to_string();
                        let arguments = &type_path.path.segments.last().unwrap().arguments;
                        if let syn::PathArguments::AngleBracketed(args) = arguments {
                            if let Some(inner_ty) = args.args.first() {
                                let inner_ty_str = quote!(#inner_ty).to_string();
                                params.push(quote! {
                                    if self.#ident.eq(&None) {
                                        map.insert(#field_name.as_bytes().to_vec(), r2d2_mysql::mysql::Value::NULL);
                                    }
                                });
                                if inner_ty_str.eq("String") {
                                    stmts.push(quote! {
                                        if columns.contains(&&*#field_name) {
                                            let value = &row[#field_name];
                                            match value {
                                                r2d2_mysql::mysql::Value::NULL => {}
                                                r2d2_mysql::mysql::Value::Date(year, month, day, hour, minutes, seconds, micro_seconds) => {
                                                    vo.#ident = Some(format!("{}-{}-{} {}:{}:{}.{}", year, month, day, hour, minutes, seconds, micro_seconds));
                                                }
                                                r2d2_mysql::mysql::Value::Time(_is_negative, days, hours, minutes, seconds, micro_seconds) => {
                                                    vo.#ident = Some(format!("{} {}:{}:{}.{}", days, hours, minutes, seconds, micro_seconds));
                                                }
                                                _ => {
                                                    vo.#ident = row.get(#field_name);
                                                }
                                            }
                                        }
                                    });
                                    params.push(quote! {
                                        else {
                                            map.insert(#field_name.as_bytes().to_vec(), r2d2_mysql::mysql::Value::Bytes(self.#ident.clone().unwrap().as_bytes().to_vec()));
                                        }
                                    });
                                    continue;
                                } else if inner_ty_str.eq("i8") {
                                    params.push(quote! {
                                        else {
                                            map.insert(#field_name.as_bytes().to_vec(), r2d2_mysql::mysql::Value::Int(self.#ident.unwrap().into()));
                                        }
                                    });
                                } else if inner_ty_str.eq("u8") {
                                    params.push(quote! {
                                        else {
                                            map.insert(#field_name.as_bytes().to_vec(), r2d2_mysql::mysql::Value::UInt(self.#ident.unwrap()));
                                        }
                                    });
                                } else if inner_ty_str.eq("i16") {
                                    params.push(quote! {
                                        else {
                                            map.insert(#field_name.as_bytes().to_vec(), r2d2_mysql::mysql::Value::Int(self.#ident.unwrap()));
                                        }
                                    });
                                } else if inner_ty_str.eq("u16") {
                                    params.push(quote! {
                                        else {
                                            map.insert(#field_name.as_bytes().to_vec(), r2d2_mysql::mysql::Value::UInt(self.#ident.unwrap()));
                                        }
                                    });
                                } else if inner_ty_str.eq("i32") {
                                    params.push(quote! {
                                        else {
                                            map.insert(#field_name.as_bytes().to_vec(), r2d2_mysql::mysql::Value::Int(self.#ident.unwrap()));
                                        }
                                    });
                                } else if inner_ty_str.eq("u32") {
                                    params.push(quote! {
                                        else {
                                            map.insert(#field_name.as_bytes().to_vec(), r2d2_mysql::mysql::Value::UInt(self.#ident.unwrap()));
                                        }
                                    });
                                } else if inner_ty_str.eq("i64") {
                                    params.push(quote! {
                                        else {
                                            map.insert(#field_name.as_bytes().to_vec(), r2d2_mysql::mysql::Value::Int(self.#ident.unwrap()));
                                        }
                                    });
                                } else if inner_ty_str.eq("u64") {
                                    params.push(quote! {
                                        else {
                                            map.insert(#field_name.as_bytes().to_vec(), r2d2_mysql::mysql::Value::UInt(self.#ident.unwrap()));
                                        }
                                    });
                                } else if inner_ty_str.eq("isize") {
                                    params.push(quote! {
                                        else {
                                            map.insert(#field_name.as_bytes().to_vec(), r2d2_mysql::mysql::Value::Int(self.#ident.unwrap()));
                                        }
                                    });
                                } else if inner_ty_str.eq("usize") {
                                    params.push(quote! {
                                        else {
                                            map.insert(#field_name.as_bytes().to_vec(), r2d2_mysql::mysql::Value::UInt(self.#ident.unwrap()));
                                        }
                                    });
                                } else if inner_ty_str.eq("f32") {
                                    params.push(quote! {
                                        else {
                                            map.insert(#field_name.as_bytes().to_vec(), r2d2_mysql::mysql::Value::Float(self.#ident.unwrap()));
                                        }
                                    });
                                } else if inner_ty_str.eq("f64") {
                                    params.push(quote! {
                                        else {
                                            map.insert(#field_name.as_bytes().to_vec(), r2d2_mysql::mysql::Value::Double(self.#ident.unwrap()));
                                        }
                                    });
                                }
                                stmts.push(quote! {
                                    if columns.contains(&&*#field_name) {
                                        let value = &row[#field_name];
                                        if !r2d2_mysql::mysql::Value::NULL.eq(value) {
                                            vo.#ident = row.get(#field_name);
                                        }
                                    }
                                });
                            }
                        }
                    }
                }
            }

            let mut insert_sql = String::from("INSERT INTO `");
            insert_sql = insert_sql.add(table_name);
            insert_sql = insert_sql.add("` (");
            for type_name in type_names.clone() {
                if type_name.eq(primary_key) {
                    continue;
                }
                insert_sql = insert_sql.add("`");
                insert_sql = insert_sql.add(&*type_name);
                insert_sql = insert_sql.add("`,");
            }
            insert_sql.remove(insert_sql.len() - 1);
            insert_sql = insert_sql.add(") VALUES (");
            for type_name in type_names {
                if type_name.eq(primary_key) {
                    continue;
                }
                insert_sql = insert_sql.add(":");
                insert_sql = insert_sql.add(&*type_name);
                insert_sql = insert_sql.add(",");
            }
            insert_sql.remove(insert_sql.len() - 1);
            insert_sql = insert_sql.add(")");

            let impl_fn = quote! {
                use std::ops::Add;
                #input_tokens

                impl r2d2_mysql_batis::entity::Entity for #struct_name {
                    fn table_name() -> &'static str {
                        #table_name
                    }

                    fn primary_key() -> &'static str {
                        #primary_key
                    }

                    fn insert_sql() -> &'static str {
                        #insert_sql
                    }

                    fn update_by_id_sql(&self) -> String {
                        let mut update_sql = String::from("UPDATE `");
                        update_sql = update_sql.add(#table_name);
                        update_sql = update_sql.add("` SET");
                        #(#update_sql)*
                        update_sql.remove(update_sql.len() - 1);
                        update_sql = update_sql.add(" WHERE `");
                        update_sql = update_sql.add(#primary_key);
                        update_sql = update_sql.add("` = :user_id LIMIT 1");
                        update_sql
                    }

                    fn params(&self) -> r2d2_mysql::mysql::Params {
                        let mut map = std::collections::HashMap::new();
                        #(#params)*
                        r2d2_mysql::mysql::Params::Named(map)
                    }
                }

                impl r2d2_mysql_batis::service::Service for #struct_name {
                    fn set_primary_key(&mut self, id: u64) {
                        self.#primary_key_name = Some(id);
                    }
                }

                impl r2d2_mysql::mysql::prelude::FromRow for #struct_name {
                    fn from_row(row: r2d2_mysql::mysql::Row) -> Self where Self: Sized {
                        let mut vo = Self {
                            #(#assignment)*
                        };
                        let mut columns = vec![];
                        for column in row.columns_ref() {
                            columns.push(std::str::from_utf8(column.name_ref()).unwrap());
                        }
                        #(#stmts)*
                        vo
                    }

                    fn from_row_opt(row: r2d2_mysql::mysql::Row) -> Result<Self, r2d2_mysql::mysql::FromRowError> where Self: Sized {
                        let mut vo = Self {
                            #(#assignment)*
                        };
                        let mut columns = vec![];
                        for column in row.columns_ref() {
                            columns.push(std::str::from_utf8(column.name_ref()).unwrap());
                        }
                        #(#stmts)*
                        Ok(vo)
                    }
                }
            };
            return impl_fn.into();
        }
    }
    TokenStream::default()
}