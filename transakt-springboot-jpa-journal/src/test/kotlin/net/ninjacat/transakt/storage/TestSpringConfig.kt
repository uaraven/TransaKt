package net.ninjacat.transakt.storage

import org.springframework.boot.autoconfigure.EnableAutoConfiguration
import org.springframework.boot.autoconfigure.domain.EntityScan
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration
import org.springframework.data.jpa.repository.config.EnableJpaRepositories

@Configuration
@ComponentScan(basePackageClasses = [TransactionJournalStorage::class, SpringJpaRepositoryStorage::class])
@EntityScan(basePackageClasses = [StageEntity::class])
@EnableAutoConfiguration
@EnableJpaRepositories(basePackageClasses = [StageRepository::class])
open class TestSpringConfig
