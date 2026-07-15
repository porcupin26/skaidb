//! Role-based access control (SPEC §8.2).
//!
//! Privileges are granted to roles on objects (the whole cluster, or a specific
//! table). Roles inherit privileges from roles granted to them. A role holding
//! `Admin` on the global object is a superuser. Granularity is keyspace/table;
//! row- and column-level control is a later phase.

use std::collections::{BTreeMap, BTreeSet};

/// An access privilege (SPEC §8.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Privilege {
    Select,
    Insert,
    Update,
    Delete,
    Create,
    Drop,
    Grant,
    /// Read-only control-plane introspection (`SHOW CLUSTER`, `SHOW CONFIG`,
    /// `SHOW SLOW QUERIES`, and the matching read-only HTTP admin endpoints):
    /// lets an application role report cluster health and its effective
    /// config without an admin credential. Never authorizes a mutation.
    Monitor,
    /// Full control (superuser when held on [`Object::Global`]).
    Admin,
}

/// The object a privilege applies to.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Object {
    /// The whole cluster — a grant here covers every table.
    Global,
    /// A specific table by name.
    Table(String),
    /// Every table in one database. The store matches objects exactly; the
    /// enforcement layer widens a table check to its database (it knows the
    /// session's database, the store does not).
    Database(String),
}

#[derive(Debug, Clone, Default)]
struct Role {
    grants: BTreeMap<Object, BTreeSet<Privilege>>,
    inherits: BTreeSet<String>,
}

/// Canonical lowercase name of a privilege (stable storage/wire form).
pub fn privilege_name(p: Privilege) -> &'static str {
    match p {
        Privilege::Select => "select",
        Privilege::Insert => "insert",
        Privilege::Update => "update",
        Privilege::Delete => "delete",
        Privilege::Create => "create",
        Privilege::Drop => "drop",
        Privilege::Grant => "grant",
        Privilege::Monitor => "monitor",
        Privilege::Admin => "admin",
    }
}

/// Parse a privilege from its canonical name (case-insensitive).
pub fn privilege_from_name(s: &str) -> Option<Privilege> {
    Some(match s.to_ascii_lowercase().as_str() {
        "select" => Privilege::Select,
        "insert" => Privilege::Insert,
        "update" => Privilege::Update,
        "delete" => Privilege::Delete,
        "create" => Privilege::Create,
        "drop" => Privilege::Drop,
        "grant" => Privilege::Grant,
        "monitor" => Privilege::Monitor,
        "admin" => Privilege::Admin,
        _ => return None,
    })
}

/// Errors from RBAC operations.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RbacError {
    #[error("role {0:?} does not exist")]
    NoSuchRole(String),
    #[error("role {0:?} already exists")]
    RoleExists(String),
}

/// An in-memory set of roles and their grants.
#[derive(Debug, Clone, Default)]
pub struct RoleStore {
    roles: BTreeMap<String, Role>,
}

impl RoleStore {
    pub fn new() -> Self {
        RoleStore::default()
    }

    /// Create a role. Errors if it already exists.
    pub fn create_role(&mut self, name: &str) -> Result<(), RbacError> {
        if self.roles.contains_key(name) {
            return Err(RbacError::RoleExists(name.to_string()));
        }
        self.roles.insert(name.to_string(), Role::default());
        Ok(())
    }

    /// Create a superuser role (holds `Admin` on the global object). Used to
    /// bootstrap the configured superuser on first start (SPEC §8.2).
    pub fn create_superuser(&mut self, name: &str) {
        let role = self.roles.entry(name.to_string()).or_default();
        role.grants
            .entry(Object::Global)
            .or_default()
            .insert(Privilege::Admin);
    }

    pub fn drop_role(&mut self, name: &str) -> Result<(), RbacError> {
        self.roles
            .remove(name)
            .map(|_| ())
            .ok_or_else(|| RbacError::NoSuchRole(name.to_string()))
    }

    pub fn role_exists(&self, name: &str) -> bool {
        self.roles.contains_key(name)
    }

    /// Grant `privilege` on `object` to `role`.
    pub fn grant(
        &mut self,
        role: &str,
        privilege: Privilege,
        object: Object,
    ) -> Result<(), RbacError> {
        let r = self
            .roles
            .get_mut(role)
            .ok_or_else(|| RbacError::NoSuchRole(role.to_string()))?;
        r.grants.entry(object).or_default().insert(privilege);
        Ok(())
    }

    /// Revoke `privilege` on `object` from `role` (direct grants only).
    pub fn revoke(
        &mut self,
        role: &str,
        privilege: Privilege,
        object: &Object,
    ) -> Result<(), RbacError> {
        let r = self
            .roles
            .get_mut(role)
            .ok_or_else(|| RbacError::NoSuchRole(role.to_string()))?;
        if let Some(set) = r.grants.get_mut(object) {
            set.remove(&privilege);
        }
        Ok(())
    }

    /// Grant membership: `member` inherits everything `parent` can do.
    pub fn grant_role(&mut self, member: &str, parent: &str) -> Result<(), RbacError> {
        if !self.roles.contains_key(parent) {
            return Err(RbacError::NoSuchRole(parent.to_string()));
        }
        let m = self
            .roles
            .get_mut(member)
            .ok_or_else(|| RbacError::NoSuchRole(member.to_string()))?;
        m.inherits.insert(parent.to_string());
        Ok(())
    }

    /// Remove a role-inheritance edge. Missing role/edge is a no-op.
    pub fn revoke_role(&mut self, member: &str, parent: &str) {
        if let Some(m) = self.roles.get_mut(member) {
            m.inherits.remove(parent);
        }
    }

    /// Every `(role, privilege, object)` grant plus `(role, "ROLE", parent)`
    /// inheritance edge — the `SHOW GRANTS` view.
    pub fn grants(&self, only: Option<&str>) -> Vec<(String, String, String)> {
        let mut out = Vec::new();
        for (name, role) in &self.roles {
            if only.is_some_and(|o| o != name) {
                continue;
            }
            for (object, privs) in &role.grants {
                for p in privs {
                    out.push((
                        name.clone(),
                        privilege_name(*p).to_string(),
                        match object {
                            Object::Global => "*".to_string(),
                            Object::Table(t) => t.clone(),
                            Object::Database(d) => format!("db:{d}"),
                        },
                    ));
                }
            }
            for parent in &role.inherits {
                out.push((name.clone(), "ROLE".to_string(), parent.clone()));
            }
        }
        out
    }

    /// Whether `role` may perform `privilege` on `object`, following role
    /// inheritance. `Admin`-on-global is a superuser; a privilege granted on
    /// the global object covers every table.
    pub fn has_privilege(&self, role: &str, privilege: Privilege, object: &Object) -> bool {
        let mut visited = BTreeSet::new();
        self.check(role, privilege, object, &mut visited)
    }

    fn check(
        &self,
        role: &str,
        privilege: Privilege,
        object: &Object,
        visited: &mut BTreeSet<String>,
    ) -> bool {
        if !visited.insert(role.to_string()) {
            return false; // cycle guard
        }
        let Some(r) = self.roles.get(role) else {
            return false;
        };

        // Superuser: Admin on the global object grants everything.
        if grants_contains(&r.grants, &Object::Global, Privilege::Admin) {
            return true;
        }
        // Direct privilege on the object, or the same privilege granted globally.
        if grants_contains(&r.grants, object, privilege)
            || grants_contains(&r.grants, &Object::Global, privilege)
        {
            return true;
        }
        // Admin on the specific object also implies the privilege.
        if grants_contains(&r.grants, object, Privilege::Admin) {
            return true;
        }
        // Inherited roles.
        r.inherits
            .iter()
            .any(|parent| self.check(parent, privilege, object, visited))
    }
}

fn grants_contains(
    grants: &BTreeMap<Object, BTreeSet<Privilege>>,
    object: &Object,
    privilege: Privilege,
) -> bool {
    grants
        .get(object)
        .is_some_and(|set| set.contains(&privilege))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table(name: &str) -> Object {
        Object::Table(name.to_string())
    }

    #[test]
    fn direct_grant() {
        let mut s = RoleStore::new();
        s.create_role("reader").unwrap();
        s.grant("reader", Privilege::Select, table("users"))
            .unwrap();
        assert!(s.has_privilege("reader", Privilege::Select, &table("users")));
        assert!(!s.has_privilege("reader", Privilege::Insert, &table("users")));
        assert!(!s.has_privilege("reader", Privilege::Select, &table("orders")));
    }

    #[test]
    fn superuser_can_do_anything() {
        let mut s = RoleStore::new();
        s.create_superuser("admin");
        assert!(s.has_privilege("admin", Privilege::Drop, &table("anything")));
        assert!(s.has_privilege("admin", Privilege::Select, &Object::Global));
    }

    #[test]
    fn global_grant_covers_all_tables() {
        let mut s = RoleStore::new();
        s.create_role("auditor").unwrap();
        s.grant("auditor", Privilege::Select, Object::Global)
            .unwrap();
        assert!(s.has_privilege("auditor", Privilege::Select, &table("a")));
        assert!(s.has_privilege("auditor", Privilege::Select, &table("b")));
        assert!(!s.has_privilege("auditor", Privilege::Insert, &table("a")));
    }

    #[test]
    fn inheritance_chains() {
        let mut s = RoleStore::new();
        s.create_role("base").unwrap();
        s.create_role("mid").unwrap();
        s.create_role("user").unwrap();
        s.grant("base", Privilege::Select, table("t")).unwrap();
        s.grant_role("mid", "base").unwrap();
        s.grant_role("user", "mid").unwrap();
        assert!(s.has_privilege("user", Privilege::Select, &table("t")));
    }

    #[test]
    fn inheritance_cycle_is_safe() {
        let mut s = RoleStore::new();
        s.create_role("a").unwrap();
        s.create_role("b").unwrap();
        s.grant_role("a", "b").unwrap();
        s.grant_role("b", "a").unwrap();
        // No privilege granted anywhere → false, and no infinite loop.
        assert!(!s.has_privilege("a", Privilege::Select, &table("t")));
    }

    #[test]
    fn database_grants_are_exact_objects() {
        let mut s = RoleStore::new();
        s.create_role("analyst").unwrap();
        s.grant("analyst", Privilege::Select, Object::Database("sales".into()))
            .unwrap();
        assert!(s.has_privilege(
            "analyst",
            Privilege::Select,
            &Object::Database("sales".into())
        ));
        assert!(!s.has_privilege(
            "analyst",
            Privilege::Select,
            &Object::Database("hr".into())
        ));
        // The store matches objects exactly; widening a table check to its
        // database happens at the enforcement layer.
        assert!(!s.has_privilege("analyst", Privilege::Select, &table("orders")));
        // A global grant still covers database objects.
        s.grant("analyst", Privilege::Insert, Object::Global).unwrap();
        assert!(s.has_privilege(
            "analyst",
            Privilege::Insert,
            &Object::Database("hr".into())
        ));
    }

    #[test]
    fn revoke_removes_privilege() {
        let mut s = RoleStore::new();
        s.create_role("r").unwrap();
        s.grant("r", Privilege::Insert, table("t")).unwrap();
        assert!(s.has_privilege("r", Privilege::Insert, &table("t")));
        s.revoke("r", Privilege::Insert, &table("t")).unwrap();
        assert!(!s.has_privilege("r", Privilege::Insert, &table("t")));
    }

    #[test]
    fn errors_on_missing_roles() {
        let mut s = RoleStore::new();
        assert_eq!(
            s.grant("ghost", Privilege::Select, Object::Global),
            Err(RbacError::NoSuchRole("ghost".into()))
        );
        s.create_role("dup").unwrap();
        assert_eq!(
            s.create_role("dup"),
            Err(RbacError::RoleExists("dup".into()))
        );
    }
}
