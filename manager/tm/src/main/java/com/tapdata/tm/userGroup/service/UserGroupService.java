package com.tapdata.tm.userGroup.service;

import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.userGroup.dto.UserGroupDto;
import com.tapdata.tm.userGroup.entity.UserGroupEntity;
import com.tapdata.tm.userGroup.repository.UserGroupRepository;
import com.tapdata.tm.config.security.UserDetail;
import java.util.Arrays;
import java.util.List;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

/**
 * @Author:
 * @Date: 2021/12/01
 * @Description:
 */
@Service
@Slf4j
public class UserGroupService extends BaseService<UserGroupDto, UserGroupEntity, ObjectId, UserGroupRepository> {

	private static final String GID_PREFIX = "GID";

	private static final String[] GID_KEYS = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".split("");

	private final UserService userService;

	public UserGroupService(@NonNull UserGroupRepository repository, UserService userService) {
        super(repository, UserGroupDto.class, UserGroupEntity.class);
		this.userService = userService;
	}

    protected void beforeSave(UserGroupDto userGroup, UserDetail user) {

    }

	@Override
	public <T extends BaseDto> UserGroupDto save(UserGroupDto dto, UserDetail userDetail) {
		UserGroupDto groupDto;
		if (StringUtils.isNotBlank(dto.getParentGid())){
			Query query = Query.query(Criteria.where("parent_gid").is(dto.getParentGid()));
			groupDto = findOne(query.with(Sort.by(Sort.Direction.DESC, "gid")));
			if (groupDto == null){
				groupDto = new UserGroupDto();
				groupDto.setParentGid(dto.getParentGid());
			}
		}else {
			Query query = Query.query(Criteria.where("parent_gid").exists(false));
			groupDto = findOne(query.with(Sort.by(Sort.Direction.DESC, "gid")));
		}

		dto.setGid(getGid(groupDto));
		return super.save(dto, userDetail);
	}

	@Override
	public boolean deleteById(ObjectId id, UserDetail userDetail) {

		UserGroupDto userGroupDto = findById(id);
		if (userGroupDto != null){
			long count = userService.count(Query.query(Criteria.where("listtags.gid").regex(userGroupDto.getGid())));
			if (count > 0){
				throw new BizException("UserGroup.Exists.User");
			}
			return super.deleteAll(Query.query(Criteria.where("gid").regex("^" + userGroupDto.getGid() + ".*"))) > 0;
		}

		return false;
	}

	private String getGid(UserGroupDto groupDto){
		String current = "000";
		if (groupDto != null && StringUtils.isNotBlank(groupDto.getGid()) && groupDto.getGid().length() >= 3){
			current = groupDto.getGid().substring(groupDto.getGid().length() - 3);
		}
		String[] split = current.split("");
		List<String> list = Arrays.asList(GID_KEYS);
		int firstIdx = list.indexOf(split[0]);
		int secondIdx = list.indexOf(split[1]);
		int threeIdx = list.indexOf(split[2]);
		if (++threeIdx >= GID_KEYS.length){
			threeIdx = 0;
			secondIdx++;
		}
		if (secondIdx >= GID_KEYS.length){
			secondIdx = 0;
			firstIdx++;
		}
		if (firstIdx >= GID_KEYS.length){
			firstIdx = 0;
		}

		return (groupDto != null && StringUtils.isNotBlank(groupDto.getParentGid()) ? groupDto.getParentGid() : GID_PREFIX)
				+ GID_KEYS[firstIdx] + GID_KEYS[secondIdx] + GID_KEYS[threeIdx];
	}
}