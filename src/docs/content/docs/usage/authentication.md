+++
[menu.docs]
name = "Activate authentication for the web UI"
weight = 100
identifier = "auth"
parent = "usage"
+++

# Activate authentication for the web UI

Authentication can be activated in Reaper for the web UI only. It relies on [Apache Shiro](https://shiro.apache.org/), which allows to store users and password in files, databases or connect through LDAP and Active Directory out of the box. 

To activate authentication, add the following block to your Reaper yaml file : 

```
accessControl:
  sessionTimeout: PT10M
  shiro:
    iniConfigs: ["file:/path/to/shiro.ini"]
```

Create a `shiro.ini` file and adapt it from the following sample : 

```
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

Then start Reaper.

Both the REST API and the `/webui/login.html` pages will be accessible anonymously, but all other pages will require to be authenticated.

 


