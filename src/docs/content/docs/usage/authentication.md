+++
[menu.docs]
name = "Activating Web UI Authentication"
weight = 100
identifier = "auth"
parent = "usage"
+++

# Web UI Authentication

Authentication is activated in Reaper by default. It relies on [Apache Shiro](https://shiro.apache.org/), which allows to store users and password in files, databases or connect through LDAP and Active Directory out of the box. The default authentication uses the dummy username and password as found in the default [shiro.ini](https://github.com/thelastpickle/cassandra-reaper/blob/master/src/server/src/main/resources/shiro.ini). It is expected you override this in a production environment.

This default Shiro authentication configuration is referenced via the  following block in the Reaper yaml file : 

```ini
accessControl:
  sessionTimeout: PT10M
  shiro:
    iniConfigs: ["classpath:shiro.ini"]
```

## With clear passwords

Copy the default `shiro.ini` file and adapt it overriding the "users" section : 

```ini
…

[users]
user1 = password1
user2 = password2

…
```

## With encrypted passwords

Based on [Shiro's document on Encrypting passwords](https://shiro.apache.org/configuration.html#Configuration-EncryptingPasswords) :

```ini
[main]
authc = org.apache.shiro.web.filter.authc.PassThruAuthenticationFilter
authc.loginUrl = /webui/login.html
sha256Matcher = org.apache.shiro.authc.credential.Sha256CredentialsMatcher
iniRealm.credentialsMatcher = $sha256Matcher

[users]
john = 807A09440428C0A8AEF58BD3ECE32938B0D76E638119E47619756F5C2C20FF3A

…
```

To generate a password, you case use for example :

* From the command line :

```shell
echo -n "Hello World" | shasum -a 256
echo -n "Hello World" | sha256sum
```

* Or some language of your choice  (like Python here) :

```python
import hashlib
hash_object = hashlib.sha256(b'Hello World')
hex_dig = hash_object.hexdigest()
print(hex_dig)
a591a6d40bf420404a011733cfb7b190d62c65bf0bcda32b57b277d9ad9f146e
```

## With LDAP accounts

Based on [Shiro's LDAP realm usage](https://shiro.apache.org/static/1.2.4/apidocs/org/apache/shiro/realm/ldap/JndiLdapContextFactory.html).

An example configuration for LDAP authentication exists (commented out) in the default shiro.ini :

```

# Example LDAP realm, see https://shiro.apache.org/static/1.2.4/apidocs/org/apache/shiro/realm/ldap/JndiLdapContextFactory.html
ldapRealm = org.apache.shiro.realm.ldap.JndiLdapRealm
ldapRealm.userDnTemplate = uid={0},ou=users,dc=cassandra-reaper,dc=io
ldapRealm.contextFactory.url = ldap://ldapHost:389
;ldapRealm.contextFactory.authenticationMechanism = DIGEST-MD5
;ldapRealm.contextFactory.systemUsername = cn=Manager, dc=example, dc=com
;ldapRealm.contextFactory.systemPassword = secret
;ldapRealm.contextFactory.environment[java.naming.security.credentials] = ldap_password

```


## Accessing Reaper via the command line `spreaper`

The RESTful endpoints to Reaper can also be authenticated using JWT (Java Web Token). A token can be generated from the `/jwt` url.
