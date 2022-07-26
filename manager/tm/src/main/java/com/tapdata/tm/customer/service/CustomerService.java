package com.tapdata.tm.customer.service;

import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.customer.dto.CustomerDto;
import com.tapdata.tm.customer.entity.CustomerEntity;
import com.tapdata.tm.customer.repository.CustomerRepository;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.user.entity.User;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.stereotype.Service;

/**
 * @Author:
 * @Date: 2022/02/14
 * @Description:
 */
@Service
@Slf4j
public class CustomerService extends BaseService<CustomerDto, CustomerEntity, ObjectId, CustomerRepository> {
    public CustomerService(@NonNull CustomerRepository repository) {
        super(repository, CustomerDto.class, CustomerEntity.class);
    }

    protected void beforeSave(CustomerDto customer, UserDetail user) {

    }

    public CustomerDto createDefaultCustomer(User user) {

        CustomerEntity customerEntity = new CustomerEntity();

        customerEntity.setPhone(user.getPhone());
        customerEntity.setContact(user.getUsername());

        customerEntity = repository.getMongoOperations().insert(customerEntity, repository.getCollectionName());

        return convertToDto(customerEntity, CustomerDto.class);
    }
}
