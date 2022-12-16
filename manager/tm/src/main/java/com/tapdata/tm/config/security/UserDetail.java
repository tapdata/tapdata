package com.tapdata.tm.config.security;

import com.tapdata.tm.user.entity.Notification;
import com.tapdata.tm.user.entity.User;
import lombok.Getter;
import lombok.Setter;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.util.Assert;

import java.io.Serializable;
import java.util.*;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/9/11 4:26 下午
 * @description
 */
@Getter
@Setter
public class UserDetail implements Serializable, UserDetails {
	private static final long serialVersionUID = -8293875593519087622L;
	private String userId;
	private String customerId;
	private String username;
	private String password;
	private String accessCode;
	private Set<SimpleGrantedAuthority> authorities;
	private boolean accountNonExpired;
	private boolean accountNonLocked;
	private boolean credentialsNonExpired;
	private boolean enabled;
	private String email;
	private String phone;
	private Notification notification;
	private String externalUserId;

	private ThreadLocal<Boolean> authenticationThreadLocal = InheritableThreadLocal.withInitial(() -> false);


	public UserDetail(String userId, String customerId, String username, String password, String customerType, Collection<? extends SimpleGrantedAuthority> authorities) {
		this(userId, customerId, username, password, customerType, null, true, true, true, true, authorities);
	}
	public UserDetail(String userId, String customerId, String username, String password, Collection<? extends SimpleGrantedAuthority> authorities) {
		this(userId, customerId, username, password, null, null, true, true, true, true, authorities);
	}

	public UserDetail(String userId, String customerId, String username, String password, String customerType, String accessCode, boolean enabled, boolean accountNonExpired, boolean credentialsNonExpired, boolean accountNonLocked, Collection<? extends SimpleGrantedAuthority> authorities) {
		this.userId = userId;
		this.customerId = customerId;
		this.accessCode = accessCode;

		if (((username == null) || "".equals(username)) || (password == null)) {
			throw new IllegalArgumentException(
				"Cannot pass null or empty values to constructor");
		}

		this.username = username;
		this.password = password;
		this.enabled = enabled;
		this.email = null;
		this.phone = null;
		this.accountNonExpired = accountNonExpired;
		this.credentialsNonExpired = credentialsNonExpired;
		this.accountNonLocked = accountNonLocked;
		this.authorities = Collections.unmodifiableSet(sortAuthorities(authorities));
	}

	public <T> UserDetail(User user, Collection<SimpleGrantedAuthority> authorities) {

		if (user == null) {
			throw new IllegalArgumentException(
					"Cannot pass null or empty values to constructor");
		}

		this.userId = user.getId().toHexString();
		this.customerId = user.getCustomId();
		this.accessCode = user.getAccessCode();
		this.username = user.getUsername();
		this.password = user.getPassword();
		this.email = user.getEmail();
		this.phone = user.getPhone();
		this.enabled = true;
		this.accountNonExpired = true;
		this.credentialsNonExpired = true;
		this.accountNonLocked = true;
		this.authorities = Collections.unmodifiableSet(sortAuthorities(authorities));
		this.notification=user.getNotification();
		this.externalUserId=user.getExternalUserId();
	}

	public boolean isRoot() {
		return getAuthorities() != null &&
			getAuthorities().stream().anyMatch(grantedAuthority -> "ADMIN".equals(grantedAuthority.getAuthority()));
	}

	public void setFreeAuth() {
		authenticationThreadLocal.set(true);
	}

	public boolean isFreeAuth() {
		return authenticationThreadLocal.get();
	}

	public Collection<? extends GrantedAuthority> getAuthorities() {
		return authorities;
	}

	public String getPassword() {
		return password;
	}

	public String getUsername() {
		return Objects.isNull(username) ? email : username;
	}

	public boolean isEnabled() {
		return enabled;
	}

	private static SortedSet<SimpleGrantedAuthority> sortAuthorities(
		Collection<? extends SimpleGrantedAuthority> authorities) {
		Assert.notNull(authorities, "Cannot pass a null GrantedAuthority collection");
		// Ensure array iteration order is predictable (as per
		// UserDetails.getAuthorities() contract and SEC-717)
		SortedSet<SimpleGrantedAuthority> sortedAuthorities = new TreeSet<>(
			new AuthorityComparator());

		for (SimpleGrantedAuthority grantedAuthority : authorities) {
			Assert.notNull(grantedAuthority,
				"GrantedAuthority list cannot contain any null elements");
			sortedAuthorities.add(grantedAuthority);
		}

		return sortedAuthorities;
	}
	private static class AuthorityComparator implements Comparator<SimpleGrantedAuthority>,
		Serializable {
		private static final long serialVersionUID = 530L;

		public int compare(SimpleGrantedAuthority g1, SimpleGrantedAuthority g2) {
			// Neither should ever be null as each entry is checked before adding it to
			// the set.
			// If the authority is null, it is a custom authority and should precede
			// others.
			if (g2.getAuthority() == null) {
				return -1;
			}

			if (g1.getAuthority() == null) {
				return 1;
			}

			return g1.getAuthority().compareTo(g2.getAuthority());
		}
	}
}
