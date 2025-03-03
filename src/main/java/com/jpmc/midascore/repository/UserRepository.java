package com.jpmc.midascore.repository;

import com.jpmc.midascore.entity.UserRecord;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.math.BigDecimal;
import java.util.Optional;

public interface UserRepository extends JpaRepository<UserRecord, Long> {

    // Fix: Ensuring findById returns Optional<UserRecord>
    @Override
    Optional<UserRecord> findById(Long id);

    // Fetch user by name
    Optional<UserRecord> findByName(String name);

    // Fetch balance of a user by name
    @Query("SELECT u.balance FROM UserRecord u WHERE u.name = :name")
    BigDecimal findBalanceByName(@Param("name") String name);
}