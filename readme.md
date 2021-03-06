# TransaKt

[![Build Status](https://travis-ci.org/uaraven/TransaKt.svg?branch=master)](https://travis-ci.org/uaraven/TransaKt)
[![Coverage Status](https://coveralls.io/repos/github/uaraven/TransaKt/badge.svg?branch=coveralls-integration)](https://coveralls.io/github/uaraven/TransaKt?branch=coveralls-integration)

## Description

Experiment on creating transaction manager for handling distributed(?) transactions written in Kotlin.
TransaKt is highly opinionated and, and beware, just an experiment, not proven by any production deployment.

Intended usage is to combine several changes to independent services into one atomic transaction 
with ability to rollback* already performed actions in the event of failure.

\*Rollback in this document means execution of another action which can compensate/cancel out previous action.

## Usage

Firstly, set up transaction log storage. TransaKt comes with memory-based, file-based and springboot-data-based storage 
built in. 

In-memory storage is only intended for usage with unit-tests.
Spring-based storage uses spring-data CrudRepository to store transaction journal in database. 

See unit tests for more details.

Then, create a new transaction and begin it with `begin` method.  

Each transaction consists of stages. Stage is a unit of execution that modifies state of an external component and 
can be rolled back. Transaction manager writes transaction log entry after every successful stage execution, before moving on.  

Each stage must implement `TxnStage` interface and result in either success of failure, represented 
by `Result<F, S>` type. `Result` type is an implementation of `Either` monad and has two possible values - 
`Result.Success` and `Result.Failure`.

If stage returns `Result.Failure` then transaction will terminate and will attempt to roll back all previously completed
stages. Stage must never throw exceptions, all failures must be represented as `Result.Failure`.

When running in the context of transaction (inside begin block) stages can be executed with `execute(TxnStage<L, R>)` method. 
`execute` will unwrap result of successful stage allowing to write transaction logic in simple imperative style.

```kotlin
    val txnLogStorage = FileTxnStorage(Paths.get("/var/txn/")) // file-based transaction log
    // or 
    // @Autowired lateinit val txnLogStorage: SpringJpaRepositoryStorage
    
    val txn = Transaction(txnLogStorage)
    
    val completedTxn = txn.begin { // starts transaction
        val card = execute(getCard(userId)) // retrieve card for user
        val payment = execute(processPayment(card)) // process payment for card contents
        return execute(completeOrder(userId, payment, card)) // complete order 
    }
    
    val result = completedTxn() // retrieve results of transaction
    // val result = completedTxn.result() will also work
    
    when(result) {
        is Success -> orderSuccessful(result.value)
        is Failure -> logFailure(result.failure)
    }
    
```

## Limitations and requirements

### Writing stage classes

Each stage must be an class implementing `TxnStage<F, S>` interface. `apply(): Result<F, S>` method contains
stage logic. `rollback()` method implements action required to compensate for actions in `apply()`.

When using build-in file-backed transaction log, stage classes must be JSON-serializable with `Jackson` library. 
This requirement might be relaxed when implementing custom transaction log.

Before performing rollback, transaction manager loads all the stages for failed transaction from transaction log.
Stage objects are deserialized from their persistent representation. Stage class must either serialize all the data 
that might be required for `rollback()` action or a custom deserializer must be used to inject all required dependencies 
into the stage when reading it from transaction log.

### Concurrency

TransaKt does not support concurrent execution of stages. Given that Kotlin 1.3 with official support for coroutines
is released there is a possibility that `execute()` method might be accepting suspending functions to run stages
concurrently. This might be viable for a cases where rollback ordering is not required. 
