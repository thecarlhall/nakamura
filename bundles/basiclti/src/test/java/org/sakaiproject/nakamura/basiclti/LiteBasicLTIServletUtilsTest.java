package org.sakaiproject.nakamura.basiclti;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.sakaiproject.nakamura.api.lite.Session;
import org.sakaiproject.nakamura.api.lite.accesscontrol.Permission;
import org.sakaiproject.nakamura.api.lite.accesscontrol.Permissions;
import org.sakaiproject.nakamura.api.lite.authorizable.User;
import org.sakaiproject.nakamura.api.lite.content.Content;

import java.util.Set;

@RunWith(MockitoJUnitRunner.class)
public class LiteBasicLTIServletUtilsTest {
  final String property = "someProperty";
  final String adminUserId = User.ADMIN_USER;

  @Mock
  Content node;
  @Mock
  Session session;

  @Before
  public void setUp() throws Exception {
    when(node.hasProperty(property)).thenReturn(true);
    when(session.getUserId()).thenReturn(adminUserId);
  }

  /**
   * Normal use case where the node does have the property to be removed.
   */
  @Test
  public void testRemovePropertyPropertyFound() {
    LiteBasicLTIServletUtils.removeProperty(node, property);
    verify(node, atMost(1)).removeProperty(property);
  }

  /**
   * Normal use case where the node does NOT have the property to be removed.
   */
  @Test
  public void testRemovePropertyNoPropertyFound() {
    final String somePropertyNotfound = "someOtherProperty";
    LiteBasicLTIServletUtils.removeProperty(node, somePropertyNotfound);
    verify(node, never()).removeProperty(somePropertyNotfound);
    verify(node, never()).removeProperty(property);
  }

  /**
   * Normal use case where current user *is* admin user.
   */
  @Test
  public void testIsAdminUser() {
    final boolean isAdminUser = LiteBasicLTIServletUtils.isAdminUser(session);
    assertTrue(isAdminUser);
    verify(session, atLeastOnce()).getUserId();
  }

  /**
   * Normal use case where current user is NOT admin user.
   */
  @Test
  public void testIsNotAdminUser() {
    when(session.getUserId()).thenReturn("someOtherUserId");
    final boolean isAdminUser = LiteBasicLTIServletUtils.isAdminUser(session);
    assertFalse(isAdminUser);
    verify(session, atLeastOnce()).getUserId();
  }

  /**
   * Verify basic behavior of getting set of invalid permissions on a sensitive node.
   */
  @Test
  public void testGetInvalidUserPrivileges() {
    final Set<Permission> invalidPerms = LiteBasicLTIServletUtils
        .getInvalidUserPrivileges();
    assertNotNull(invalidPerms);
    assertTrue(!invalidPerms.isEmpty());
    assertTrue(invalidPerms.contains(Permissions.CAN_READ));
    assertTrue(invalidPerms.contains(Permissions.CAN_WRITE));
  }

}
