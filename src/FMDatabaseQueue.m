//
//  FMDatabaseQueue.m
//  fmdb
//
//  Created by August Mueller on 6/22/11.
//  Copyright 2011 Flying Meat Inc. All rights reserved.
//

#import "FMDatabaseQueue.h"
#import "FMDatabase.h"

/*
 
 Note: we call [self retain]; before using dispatch_sync, just incase 
 FMDatabaseQueue is released on another thread and we're in the middle of doing
 something in dispatch_sync
 
 */

static const void * const FMDatabaseQueueGCDKey = &FMDatabaseQueueGCDKey;
NSString * const FMDatabaseQueueThreadDatabaseKey = @"FMDatabaseQueueThreadDatabase";


@interface FMDatabaseQueue ()


/**
 Default database is used in case we cannot deperated databases per thread. 
 eg. The queue run database on memory.
 */
@property (readonly) FMDatabase *defaultDatabase;

/**
 Get the appropritate database for running the block in the current thread.
 @return database for running the block in the current thread.
 */
- (FMDatabase *)databaseForCurrentThread;

/**
 Clear the database before leaving thread.
 @param database Database that is being clear.
 */
- (void)poolDatbase:(FMDatabase *)database;

@end


@implementation FMDatabaseQueue

@synthesize path = _path;

- (void)setBusyRetryTimeout:(int)busyRetryTimeout {
  if (self->_busyRetryTimeout != busyRetryTimeout) {
    self->_busyRetryTimeout = busyRetryTimeout;
    
    _db.busyRetryTimeout = self.busyRetryTimeout;
  }
}

+ (instancetype)databaseQueueWithPath:(NSString*)aPath {
    
    FMDatabaseQueue *q = [[self alloc] initWithPath:aPath];
    
    FMDBAutorelease(q);
    
    return q;
}

- (instancetype)initWithPath:(NSString*)aPath {
    
    self = [super init];
    
    if (self != nil) {
        
        if (!self.defaultDatabase) {
            NSLog(@"Could not create database queue for path %@", aPath);
            FMDBRelease(self);
            return 0x00;
        }
      
        _path = FMDBReturnRetained(aPath);
        
        _queue = dispatch_queue_create([[NSString stringWithFormat:@"fmdb.%@", self] UTF8String], DISPATCH_QUEUE_CONCURRENT);
        dispatch_queue_set_specific(_queue, FMDatabaseQueueGCDKey, (__bridge void *)self, NULL);
        _poolQueue = dispatch_queue_create([[NSString stringWithFormat:@"fmdb.%@.PoolQueue", self] UTF8String], DISPATCH_QUEUE_SERIAL);
        
        _databases = [[NSMutableSet alloc] init];
    }
    
    return self;
}

- (void)dealloc {
  
    FMDBRelease(_db);
    FMDBRelease(_path);
    
    FMDBRelease(_databases);
    
    if (_queue) {
        FMDBDispatchQueueRelease(_queue);
        _queue = 0x00;
    }
    
    if (_poolQueue) {
        FMDBDispatchQueueRelease(_poolQueue);
        _poolQueue = 0x00;
    }
    
#if ! __has_feature(objc_arc)
    [super dealloc];
#endif
}

- (void)close {
    FMDBRetain(self);
    dispatch_block_t work = ^{
        [_db close];
        FMDBRelease(_db);
        _db = 0x00;
    };
    
    dispatch_sync(_poolQueue, ^{
        NSSet *databases = [_databases copy];
        for (FMDatabase *database in databases) {
            [database close];
        }
        
        [_databases removeAllObjects];
    });

    if (dispatch_get_specific(FMDatabaseQueueGCDKey) == (__bridge void *)(self)) {
        work();
    }
    else {
        dispatch_barrier_sync(_queue, work);
    }
    FMDBRelease(self);
}


#pragma mark - Operation Methods

- (void)inDatabase:(FMDatabaseOperationBlock)block {
    [self performWriterOperation:block];
}

- (void)performReaderOperation:(FMDatabaseOperationBlock)block {
    [self performDatabaseOperationWithSynchronously:YES
                                  isWriterOperation:NO
                                          operation:block];
}

- (void)performWriterOperation:(FMDatabaseOperationBlock)block {
    [self performDatabaseOperationWithSynchronously:YES
                                  isWriterOperation:YES
                                          operation:block];
}

- (void)performAsynchronouslyWriterOperation:(FMDatabaseOperationBlock)block {
    [self performDatabaseOperationWithSynchronously:NO
                                  isWriterOperation:YES
                                          operation:block];
}

- (void)performAsynchronouslyReaderOperation:(FMDatabaseOperationBlock)block {
    [self performDatabaseOperationWithSynchronously:NO
                                  isWriterOperation:NO
                                          operation:block];
}

- (void)performDatabaseOperationWithSynchronously:(BOOL)synchronously
                                isWriterOperation:(BOOL)isWritter
                                        operation:(FMDatabaseOperationBlock)block {
    FMDBRetain(self);
  
    dispatch_block_t work = ^{
        FMDatabase *database = [self databaseForCurrentThread];
      
        // If we call perform nested operation synchronously, we test if the thread has the database or not.
        NSAssert(database,
                 @"Perform the operation within the nested block without thread database!!!");
        
        block(database);
      
        if (isWritter && [database hasOpenResultSets]) {
            NSLog(@"Warning: there is at least one open result set around after performing [FMDatabaseQueue inDatabase:]");
        }
        
        [self poolDatbase:database];
        
        FMDBRelease(self);
    };
    
    void (* dispatch_function)(dispatch_queue_t, dispatch_block_t) = NULL;
    if (!synchronously) {
        dispatch_function = isWritter ? dispatch_barrier_async : dispatch_async;
    }
    else if (dispatch_get_specific(FMDatabaseQueueGCDKey) != (__bridge void *)(self)) {
        dispatch_function = isWritter ? dispatch_barrier_sync : dispatch_sync;
    }
    
    if (dispatch_function) {
        dispatch_function(_queue, work);
    }
    else {
        // If we perform synchronously and are in the private queue, we will just invoke the block instead.
        work();
    }
}


#pragma mark - Transaction Methods

- (void)inDeferredTransaction:(void (^)(FMDatabase *db, BOOL *rollback))block {
    [self performWriterDeferredTransaction:block];
}

- (void)inTransaction:(void (^)(FMDatabase *db, BOOL *rollback))block {
    [self performWriterTransaction:block];
}

- (void)performWriterTransaction:(FMDatabaseTransactionBlock)block {
    [self performWriterTransactionWithError:NULL usingBlock:block];
}

- (void)performWriterDeferredTransaction:(FMDatabaseTransactionBlock)block {
    [self performWriterDeferredTransactionWithError:NULL usingBlock:block];
}

- (BOOL)performWriterTransactionWithError:(NSError * __autoreleasing *)error
                               usingBlock:(FMDatabaseTransactionBlock)block {
    return [self performDatabaseTransactionWithDeffered:NO
                                                  error:error
                                             usingBlock:block];
}

- (BOOL)performWriterDeferredTransactionWithError:(NSError * __autoreleasing *)error
                                       usingBlock:(FMDatabaseTransactionBlock)block {
    return [self performDatabaseTransactionWithDeffered:YES
                                                  error:error
                                             usingBlock:block];
}

- (void)performAsynchronouslyWriterDeferredTransaction:(FMDatabaseTransactionBlock)block {
    [self performAsynchronouslyWriterDeferredTransaction:block completion:NULL];
}

- (void)performAsynchronouslyWriterTransaction:(FMDatabaseTransactionBlock)block
                                    completion:(FMDatabaseCompletionBlock)completion {
    [self performDatabaseTransactionAsynchronouslyWithDeffered:NO
                                                   transaction:block
                                                    completion:completion];
}

- (void)performAsynchronouslyWriterDeferredTransaction:(FMDatabaseTransactionBlock)block
                                            completion:(FMDatabaseCompletionBlock)completion {
    [self performDatabaseTransactionAsynchronouslyWithDeffered:YES
                                                   transaction:block
                                                    completion:completion];
}

- (BOOL)performDatabaseTransactionWithDeffered:(BOOL)useDeferred
                                         error:(NSError * __autoreleasing *)error
                                    usingBlock:(FMDatabaseTransactionBlock)block {
    __block BOOL success = NO;
    FMDBRetain(self);
    dispatch_block_t work = ^{
        
        FMDatabase *database = [self databaseForCurrentThread];
        
        BOOL shouldRollback = NO;
        
        if (useDeferred) {
            success = [database beginDeferredTransaction];
        }
        else {
            success = [database beginTransaction];
        }
        
        if (success) {
            block(database, &shouldRollback);
            
            if (shouldRollback) {
                success = [database rollback];
            }
            else {
                success = [database commit];
            }
        }
        
        if (!success && error) {
            *error = [database lastError];
        }
        
        [self poolDatbase:database];
    };
    
    
    if (dispatch_get_specific(FMDatabaseQueueGCDKey) == (__bridge void *)(self)) {
        work();
    }
    else {
        dispatch_barrier_sync(_queue, work);
    }
    
    FMDBRelease(self);
    
    return success;
}

- (void)performDatabaseTransactionAsynchronouslyWithDeffered:(BOOL)useDeferred
                                                 transaction:(FMDatabaseTransactionBlock)block
                                                  completion:(FMDatabaseCompletionBlock)completion {
    __block BOOL success = NO;
    __block NSError *error = nil;
    FMDBRetain(self);
    dispatch_block_t work = ^{
        
        FMDatabase *database = [self databaseForCurrentThread];
        BOOL shouldRollback = NO;
        
        if (useDeferred) {
            success = [database beginDeferredTransaction];
        }
        else {
            success = [database beginTransaction];
        }
        
        if (success) {
            block(database, &shouldRollback);
            
            if (shouldRollback) {
                success = [database rollback];
            }
            else {
                success = [database commit];
            }
        }
        
        if (!success) {
            error = [database lastError];
        }
        
        if (completion) {
            completion(success, error);
        }
        
        [self poolDatbase:database];
        FMDBRelease(self);
    };
    
    
    if (dispatch_get_specific(FMDatabaseQueueGCDKey) == (__bridge void *)(self)) {
        work();
    }
    else {
        dispatch_barrier_async(_queue, work);
    }
}

#if SQLITE_VERSION_NUMBER >= 3007000
- (NSError*)inSavePoint:(void (^)(FMDatabase *db, BOOL *rollback))block {
    
    static unsigned long savePointIdx = 0;
    __block NSError *err = 0x00;
    FMDBRetain(self);
    dispatch_block_t work = ^{
        
        FMDatabase *database = [self defaultDatabase];
        NSString *name = [NSString stringWithFormat:@"savePoint%ld", savePointIdx++];
        
        BOOL shouldRollback = NO;
        
        if ([database startSavePointWithName:name error:&err]) {
            
            block(database, &shouldRollback);
            
            if (shouldRollback) {
                [database rollbackToSavePointWithName:name error:&err];
            }
            else {
                [database releaseSavePointWithName:name error:&err];
            }
            
        }
    };
    
    if (dispatch_get_specific(FMDatabaseQueueGCDKey) == (__bridge void *)(self)) {
        work();
    }
    else {
        dispatch_barrier_sync(_queue, work);
    }
    FMDBRelease(self);
    return err;
}
#endif


#pragma mark - Private methods

- (FMDatabase *)databaseForCurrentThread {
    __block FMDatabase *database = nil;
    if (self.path) {
        dispatch_sync(_poolQueue, ^{
            database = [_databases anyObject];
            if (!database) {
                database = [FMDatabase databaseWithPath:self.path];
                database.busyRetryTimeout = self.busyRetryTimeout;
                
                if (![database open]) {
                    NSLog(@"Could not create database queue for path %@", self.path);
                }
            } else {
              [_databases removeObject:database];
            }
            FMDBRetain(database);
        });
    } else {
        database = self.defaultDatabase;
        FMDBRetain(database);
    }
    
    return FMDBReturnAutoreleased(database);
}

- (void)poolDatbase:(FMDatabase *)database {
    if (database != _db) {
        dispatch_sync(_poolQueue, ^{
            [_databases addObject:database];
        });
    }
}

- (FMDatabase *)defaultDatabase {
    if (!_db) {
        _db = [FMDatabase databaseWithPath:self.path];
        _db.allowsMultiThread = YES;
        _db.busyRetryTimeout = self.busyRetryTimeout;
        if ([_db openWithFlags:(SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX)]) {
            FMDBRetain(_db);
        } else {
            _db = nil;
        }
    }
    
    return _db;
}


@end

