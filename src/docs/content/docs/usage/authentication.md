+++
[menu.docs]
name = "Activating Web UI Authentication"
weight = 100
identifier = "auth"
parent = "usage"
+++

# Activating Web UI Authentication

Authentication can be activated in Reaper for the web UI only. It relies on [Apache Shiro](https://shiro.apache.org/), which allows to store users and password in files, databases or connect through LDAP and Active Directory out of the box. 

To activate authentication, add the following block to your Reaper yaml file : 

```ini
accessControl:
  sessionTimeout: PT10M
  shiro:
    iniConfigs: ["file:/path/to/shiro.ini"]
```

## With clear passwords

Create a `shiro.ini` file and adapt it from the following sample : 

```ini
[main]
authc = org.apache.shiro.web.filter.authc.PassThruAuthenticationFilter
authc.loginUrl = /webui/login.html

[users]
user1 = password1
user2 = password2

[urls]
# Allow anonynous access to login page (and dependencies), but no other pages
/webui/ = authc
/webui = authc
/webui/login.html = anon
/webui/*.html* = authc
/webui/*.js* = anon
/ping = anon
/login = anon
/** = anon
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


[urls]
# Allow anonynous access to login page (and dependencies), but no other pages
/webui/ = authc
/webui = authc
/webui/login.html = anon
/webui/*.html* = authc
/webui/*.js* = anon
/ping = anon
/login = anon
/** = anon
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

Then start Reaper.

Both the REST API and the `/webui/login.html` pages will be accessible anonymously, but all other pages will require to be authenticated.