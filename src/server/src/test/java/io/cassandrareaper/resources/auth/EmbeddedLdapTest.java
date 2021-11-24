/*
 *
 * Copyright 2019-2019 The Last Pickle Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.cassandrareaper.resources.auth;

import java.util.List;

import com.unboundid.ldap.sdk.LDAPInterface;
import com.unboundid.ldap.sdk.SearchResult;
import com.unboundid.ldap.sdk.SearchResultEntry;
import com.unboundid.ldap.sdk.SearchScope;
import org.junit.Rule;
import org.junit.Test;
import org.zapodot.junit.ldap.EmbeddedLdapRule;
import org.zapodot.junit.ldap.EmbeddedLdapRuleBuilder;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 * Test using objectclass: org.zapodot:embedded-ldap-junit
 */
public final class EmbeddedLdapTest {

  public static final String DOMAIN_DSN = "dc=cassandra-reaper,dc=io";

  @Rule
  public EmbeddedLdapRule embeddedLdapRule
      = EmbeddedLdapRuleBuilder.newInstance().usingDomainDsn(DOMAIN_DSN).importingLdifs("test-ldap-users.ldif").build();

  @Test
  public void shouldFindAllPersons() throws Exception {
    LDAPInterface ldapConnection = embeddedLdapRule.ldapConnection();
    SearchResult searchResult = ldapConnection.search(DOMAIN_DSN, SearchScope.SUB, "(objectClass=person)");
    assertThat(3, equalTo(searchResult.getEntryCount()));
    List<SearchResultEntry> searchEntries = searchResult.getSearchEntries();
    assertThat(searchEntries.get(0).getAttribute("cn").getValue(), equalTo("John Steinbeck"));
    assertThat(searchEntries.get(1).getAttribute("cn").getValue(), equalTo("Micha Kops"));
    assertThat(searchEntries.get(2).getAttribute("cn").getValue(), equalTo("Santa Claus"));
  }

  @Test
  public void shouldFindExactPerson0() throws Exception {
    LDAPInterface ldapConnection = embeddedLdapRule.ldapConnection();

    SearchResult searchResult = ldapConnection
        .search("cn=John Steinbeck,ou=Users,dc=cassandra-reaper,dc=io", SearchScope.SUB, "(objectClass=person)");

    assertThat(1, equalTo(searchResult.getEntryCount()));
    assertThat(searchResult.getSearchEntries().get(0).getAttribute("cn").getValue(), equalTo("John Steinbeck"));
  }

  @Test
  public void shouldFindExactPerson1() throws Exception {
    LDAPInterface ldapConnection = embeddedLdapRule.ldapConnection();

    SearchResult searchResult = ldapConnection
        .search("uid=sclaus,ou=Users,dc=cassandra-reaper,dc=io", SearchScope.SUB, "(objectClass=person)");

    assertThat(1, equalTo(searchResult.getEntryCount()));
    assertThat(searchResult.getSearchEntries().get(0).getAttribute("cn").getValue(), equalTo("Santa Claus"));
  }
}
