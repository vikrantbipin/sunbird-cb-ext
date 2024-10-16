package org.sunbird.bpreports.postgres.repository;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import org.sunbird.bpreports.postgres.entity.WfStatusEntity;

import java.util.List;

public interface WfStatusEntityRepository extends JpaRepository<WfStatusEntity, String>{

    List<WfStatusEntity> getByApplicationId(String applicationId);
}
