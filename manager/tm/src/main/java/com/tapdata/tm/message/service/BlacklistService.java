package com.tapdata.tm.message.service;

import com.tapdata.tm.message.constant.BlacklistType;
import com.tapdata.tm.message.entity.BlacklistEntity;
import com.tapdata.tm.message.repository.BlacklistRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2023/7/1 17:15
 */
@Slf4j
@Service
public class BlacklistService {

    @Autowired
    private BlacklistRepository blacklistRepository;

    /**
     * 判断邮箱是否在黑名单
     * @param email
     * @return
     */
    public boolean inBlacklist(String email) {
        Optional<BlacklistEntity> blacklist = blacklistRepository.findOne(
                Query.query(Criteria.where("email").is(email).and("enable").is(true)));
        if (blacklist.isPresent()) {
            return true;
        }

        return matchBlacklistExpressions(email);
    }

    private boolean matchBlacklistExpressions(String str) {
        List<Pattern> expressions = blacklistRepository.findAll(Query.query(
                        Criteria.where("type").is(BlacklistType.Regex.name()).and("enable").is(true)))
                .stream().map(BlacklistEntity::getExpression).map(k -> {
                    try {
                        return Pattern.compile(k);
                    } catch (Exception e) {
                        log.error("Invalid regex expression '{}'", k, e);
                        return null;
                    }
                }).filter(Objects::nonNull).collect(Collectors.toList());
        return expressions.stream().anyMatch(p -> p.matcher(str).matches());
    }

    /**
     * 判断邮箱是否在黑名单
     * @param countryCode
     * @param phone
     * @return
     */
    public boolean inBlacklist(String countryCode, String phone) {
        Criteria criteria = Criteria.where("phone").is(phone).and("enable").is(true);
        if (StringUtils.isNotBlank(countryCode)) {
            criteria.and("countryCode").is(countryCode);
        }
        Optional<BlacklistEntity> blacklist = blacklistRepository.findOne(Query.query(criteria));
        if (blacklist.isPresent()) {
            return true;
        }

        return matchBlacklistExpressions(String.format("%s%s", countryCode != null ? countryCode : "", phone));
    }

}
