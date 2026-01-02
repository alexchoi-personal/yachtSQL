#![coverage(off)]

use sqlparser::ast;
use yachtsql_common::error::{Error, Result};
use yachtsql_ir::{DclResourceType, LogicalPlan};

use super::{Planner, object_name_to_raw_string};
use crate::CatalogProvider;

impl<'a, C: CatalogProvider> Planner<'a, C> {
    pub(super) fn plan_grant(
        &self,
        privileges: &ast::Privileges,
        objects: Option<&ast::GrantObjects>,
        grantees: &[ast::Grantee],
    ) -> Result<LogicalPlan> {
        let roles = self.extract_roles(privileges)?;
        let (resource_type, resource_name) = self.extract_grant_resource(objects)?;
        let grantee_list = self.extract_grantees(grantees)?;

        Ok(LogicalPlan::Grant {
            roles,
            resource_type,
            resource_name,
            grantees: grantee_list,
        })
    }

    pub(super) fn plan_revoke(
        &self,
        privileges: &ast::Privileges,
        objects: Option<&ast::GrantObjects>,
        grantees: &[ast::Grantee],
    ) -> Result<LogicalPlan> {
        let roles = self.extract_roles(privileges)?;
        let (resource_type, resource_name) = self.extract_grant_resource(objects)?;
        let grantee_list = self.extract_grantees(grantees)?;

        Ok(LogicalPlan::Revoke {
            roles,
            resource_type,
            resource_name,
            grantees: grantee_list,
        })
    }

    fn extract_roles(&self, privileges: &ast::Privileges) -> Result<Vec<String>> {
        match privileges {
            ast::Privileges::All { .. } => Ok(vec!["ALL".to_string()]),
            ast::Privileges::Actions(actions) => {
                Ok(actions.iter().map(|a| format!("{}", a)).collect())
            }
        }
    }

    fn extract_grant_resource(
        &self,
        objects: Option<&ast::GrantObjects>,
    ) -> Result<(DclResourceType, String)> {
        match objects {
            Some(ast::GrantObjects::Schemas(schemas)) => {
                let name = schemas
                    .first()
                    .map(object_name_to_raw_string)
                    .unwrap_or_default();
                Ok((DclResourceType::Schema, name))
            }
            Some(ast::GrantObjects::Tables(tables)) => {
                let name = tables
                    .first()
                    .map(object_name_to_raw_string)
                    .unwrap_or_default();
                Ok((DclResourceType::Table, name))
            }
            Some(ast::GrantObjects::AllTablesInSchema { schemas }) => {
                let name = schemas
                    .first()
                    .map(object_name_to_raw_string)
                    .unwrap_or_default();
                Ok((DclResourceType::Schema, name))
            }
            None => Err(Error::parse_error(
                "GRANT/REVOKE requires an ON clause specifying the resource",
            )),
            _ => Err(Error::unsupported(format!(
                "Unsupported GRANT/REVOKE object type: {:?}",
                objects
            ))),
        }
    }

    fn extract_grantees(&self, grantees: &[ast::Grantee]) -> Result<Vec<String>> {
        Ok(grantees
            .iter()
            .filter_map(|g| g.name.as_ref().map(|n| format!("{}", n)))
            .collect())
    }
}
