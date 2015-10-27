//
//  CBLTimeSeries.m
//  CouchbaseLite
//
//  Created by Jens Alfke on 10/26/15.
//  Copyright © 2015 Couchbase, Inc. All rights reserved.
//

#import "CBLTimeSeries.h"
#import "CBLDatabase+Internal.h"
#import <CouchbaseLite/CouchbaseLite.h>
#import <stdio.h>
#import <sys/mman.h>


#define kMaxDocSize         (100*1024)  // Max length in bytes of a document
#define kMaxDocEventCount   1000u       // Max number of events to pack into a document

#define kTimestampsPerSecond    10000

typedef uint64_t Timestamp;

static inline Timestamp encodeTimestamp(CFAbsoluteTime time) {
    return (Timestamp)((time + kCFAbsoluteTimeIntervalSince1970) * kTimestampsPerSecond);
}

static inline NSDate* decodeTimestamp(Timestamp timestamp) {
    return [[NSDate alloc] initWithTimeIntervalSince1970: (timestamp / (double)kTimestampsPerSecond)];
}


typedef NSArray<CBLJSONDict*> EventArray;




// Internal enumerator implementation whose -nextObject method just calls a block
@interface CBLTimeSeriesEnumerator : NSEnumerator<CBLJSONDict*>
- (id) initWithBlock: (CBLJSONDict*(^)())block;
@end




@implementation CBLTimeSeries
{
    CBLDatabase* _db;
    NSString* _docType;
    dispatch_queue_t _queue;
    FILE *_out;
    NSError* _error;
    NSUInteger _eventsInFile;
    NSMutableArray* _docsToAdd;
    BOOL _synchronousFlushing;
}


- (instancetype) initWithDatabase: (CBLDatabase*)db
                          docType: (NSString*)docType
                            error: (NSError**)outError
{
    NSParameterAssert(db);
    NSParameterAssert(docType);
    self = [super init];
    if (self) {
        NSString* filename = [NSString stringWithFormat: @"TS-%@.tslog", docType];
        NSString* path = [db.dir stringByAppendingPathComponent: filename];
        _out = fopen(path.fileSystemRepresentation, "a+"); // append-only, and read
        if (!_out) {
            if (outError)
                *outError = [NSError errorWithDomain: NSPOSIXErrorDomain code: errno userInfo:nil];
            return nil;
        }
        _queue = dispatch_queue_create("CBLTimeSeries", DISPATCH_QUEUE_SERIAL);
        _db = db;
        _docType = [docType copy];
    }
    return self;
}


- (void) dealloc {
    [self stop];
}


- (void) stop {
    if (_queue) {
        dispatch_sync(_queue, ^{
            if (_out) {
                fclose(_out);
                _out = NULL;
            }
        });
        _queue = nil;
    }
    _error = nil;
}


- (BOOL) checkError: (int)err {
    if (err == 0)
        return NO;
    Warn(@"CBLTimeSeries: POSIX error %d", err);
    if (!_error) {
        _error = [NSError errorWithDomain: NSPOSIXErrorDomain code: err
                                 userInfo: @{NSLocalizedDescriptionKey: @(strerror(err))}];
    }
    return YES;
}


- (BOOL) checkWriteError {
    return [self checkError: ferror(_out)];
}


- (void) addEvent: (CBLJSONDict*)event {
    [self addEvent: event atTime: CFAbsoluteTimeGetCurrent()];
}


- (void) addEvent: (CBLJSONDict*)event atTime: (CFAbsoluteTime)time {
    Assert(event);
    dispatch_async(_queue, ^{
        NSMutableDictionary* props = [event mutableCopy];
        props[@"t"] = @(encodeTimestamp(time));
        NSData* json = [CBLJSON dataWithJSONObject: props options: 0 error: NULL];
        Assert(json);

        off_t pos = ftell(_out);
        if (pos + json.length + 20 > kMaxDocSize || _eventsInFile >= kMaxDocEventCount) {
            [self transferToDB];
            pos = 0;
        }

        if (fputs(pos==0 ? "[" : ",\n", _out) < 0
                || fwrite(json.bytes, json.length, 1, _out) < 1
                || fflush(_out) < 0)
        {
            [self checkWriteError];
        }
        ++_eventsInFile;
    });
}


- (void) flushAsync: (void(^)())onFlushed {
    dispatch_async(_queue, ^{
        if (_eventsInFile > 0 || ftell(_out) > 0) {
            [self transferToDB];
        }
        [_db doAsync: onFlushed];
    });
}


- (void) flush {
    Assert(!_synchronousFlushing);
    _synchronousFlushing = YES;
    dispatch_sync(_queue, ^{
        if (_eventsInFile > 0 || ftell(_out) > 0)
            [self transferToDB];
    });
    _synchronousFlushing = NO;
    [self saveQueuedDocs];
}


- (BOOL) transferToDB {
    if (fputs("]", _out) < 0 || fflush(_out) < 0) {
        [self checkWriteError];
        return NO;
    }

    // Parse a JSON array from the (memory-mapped) file:
    size_t length = ftell(_out);
    void* mapped = mmap(NULL, length, PROT_READ, MAP_PRIVATE, fileno(_out), 0);
    if (!mapped)
        return [self checkError: errno];
    NSData* json = [[NSData alloc] initWithBytesNoCopy: mapped length: length freeWhenDone: NO];
    NSError* jsonError;
    EventArray* events = [CBLJSON JSONObjectWithData: json
                                             options: CBLJSONReadingMutableContainers
                                               error: &jsonError];
    munmap(mapped, length);
    if (jsonError) {
        if (!_error)
            _error = jsonError;
        return NO;
    }

    // Add the events to documents in batches:
    NSUInteger count = events.count;
    for (NSUInteger pos = 0; pos < count; pos += kMaxDocEventCount) {
        NSRange range = {pos, MIN(kMaxDocEventCount, count-pos)};
        EventArray* group = [events subarrayWithRange: range];
        [self addEventsToDB: group];
    }

    // Now erase the file for subsequent events:
    fseek(_out, 0, SEEK_SET);
    ftruncate(fileno(_out), 0);
    _eventsInFile = 0;
    return YES;
}


- (NSString*) docIDForTimestamp: (Timestamp)timestamp {
    return [NSString stringWithFormat: @"TS-%@-%08llx", _docType, timestamp];
}


- (void) addEventsToDB: (EventArray*)events {
    if (events.count == 0)
        return;
    Timestamp t0 = [[events[0] objectForKey: @"t"] unsignedLongLongValue];
    NSString* docID = [self docIDForTimestamp: t0];

    // Convert all timestamps to relative:
    Timestamp t = t0;
    for (NSMutableDictionary* event in events) {
        Timestamp tnew = [event[@"t"] unsignedLongLongValue];
        if (tnew > t)
            event[@"dt"] = @(tnew - t);
        [event removeObjectForKey: @"t"];
        t = tnew;
    }
    NSDictionary* doc = @{@"_id": docID, @"type": _docType, @"t0": @(t0), @"events": events};

    // Now add to the queue of docs to be inserted:
    BOOL firstDoc = NO;
    @synchronized(self) {
        if (!_docsToAdd) {
            _docsToAdd = [NSMutableArray new];
            firstDoc = YES;
        }
        [_docsToAdd addObject: doc];
    }
    if (firstDoc && !_synchronousFlushing) {
        [_db doAsync: ^{
            [self saveQueuedDocs];
        }];
    }
}


// Must be called on db thread
- (void) saveQueuedDocs {
    NSArray* docs;
    @synchronized(self) {
        docs = _docsToAdd;
        _docsToAdd = nil;
    }
    if (docs.count > 0) {
        [_db inTransaction: ^BOOL{
            for (CBLJSONDict *doc in docs) {
                NSError* error;
                NSString* docID = doc[@"_id"];
                if (![_db[docID] putProperties: doc error: &error])
                    Warn(@"CBLTimeSeries: Couldn't save events to '%@': %@", docID, error);
            }
            return YES;
        }];
    }
}


#pragma mark - REPLICATION:


- (CBLReplication*) createPushReplication: (NSURL*)remoteURL
                          purgeWhenPushed: (BOOL)purgeWhenPushed
{
    CBLReplication* push = [_db createPushReplication: remoteURL];
    [_db setFilterNamed: @"com.couchbase.DocIDPrefix"
                asBlock: ^BOOL(CBLSavedRevision *revision, CBLJSONDict *params) {
        return [revision.document.documentID hasPrefix: params[@"prefix"]];
    }];
    push.filter = @"com.couchbase.DocIDPrefix";
    push.filterParams = @{@"prefix": [NSString stringWithFormat: @"TS-%@-", _docType]};
    push.customProperties = @{@"allNew": @YES, @"purgePushed": @(purgeWhenPushed)};
    return push;
}


#pragma mark - QUERYING:


// Returns an array of events starting at time t0 and continuing to the end of the document.
// If t0 is before the earliest recorded timestamp, or falls between two documents, returns @[].
- (EventArray*) eventsFromDocForTime: (CFAbsoluteTime)t0
                      startTimestamp: (Timestamp*)outStartTimestamp
                               error: (NSError**)outError {
    // Get the doc containing time t0. To do this we have to search _backwards_ since the doc ID
    // probably has a timestamp before t0:
    CBLQuery* q = [_db createAllDocumentsQuery];
    Timestamp timestamp0 = (t0 != 0.0) ? encodeTimestamp(t0) : 0;
    q.startKey = [self docIDForTimestamp: timestamp0];
    q.descending = YES;
    q.limit = 1;
    q.prefetch = YES;
    __block CBLQueryEnumerator* e = [q run: outError];
    if (!e)
        return nil;
    CBLQueryRow* row = e.nextRow;
    if (!row)
        return @[];
    EventArray* events = row.document[@"events"];

    // Now find the first event with t ≥ timestamp0:
    NSUInteger i0 = 0;
    uint64 ts = [row.document[@"t0"] unsignedLongLongValue];
    for (CBLJSONDict* event in events) {
        Timestamp prevts = ts;
        ts += [event[@"dt"] unsignedLongLongValue];
        if (ts >= timestamp0) {
            *outStartTimestamp = prevts;
            break;
        }
        i0++;
    }
    return [events subarrayWithRange: NSMakeRange(i0, events.count-i0)];
}


- (NSEnumerator<CBLJSONDict*>*) eventsFromTime: (CFAbsoluteTime)t0
                                        toTime: (CFAbsoluteTime)t1
                                         error: (NSError**)outError
{
    // Get the first series from the doc containing t0 (if any):
    __block EventArray* curSeries = nil;
    __block Timestamp curTimestamp = 0;
    if (t0 > 0.0) {
        curSeries = [self eventsFromDocForTime: t0
                                startTimestamp: &curTimestamp
                                         error: outError];
        if (!curSeries)
            return nil;
    }

    // Start forwards query:
    CBLQuery* q = [_db createAllDocumentsQuery];
    Timestamp startStamp;
    if (curSeries.count > 0) {
        startStamp = curTimestamp;
        for (NSDictionary* event in curSeries)
            startStamp += [event[@"dt"] unsignedLongLongValue];
        q.inclusiveStart = NO;
    } else {
        startStamp = t0 > 0 ? encodeTimestamp(t0) : 0;
    }
    Timestamp endStamp = (t1 != 0.0) ? encodeTimestamp(t1) : UINT64_MAX;

    CBLQueryEnumerator* e;
    if (startStamp < endStamp) {
        q.startKey = [self docIDForTimestamp: startStamp];
        q.endKey   = [self docIDForTimestamp: endStamp];
        e = [q run: outError];
        if (!e)
            return nil;
    }

    // OK, here is the block for the enumerator:
    __block NSUInteger curIndex = 0;
    return [[CBLTimeSeriesEnumerator alloc] initWithBlock: ^CBLJSONDict*{
        while (curIndex >= curSeries.count) {
            // Go to the next document:
            CBLQueryRow* row = e.nextRow;
            if (!row)
                return nil;
            curSeries = row.document[@"events"];
            curIndex = 0;
            curTimestamp = [row.document[@"t0"] unsignedLongLongValue];
        }
        // Return the next event from curSeries:
        CBLJSONDict* event = curSeries[curIndex++];
        curTimestamp += [event[@"dt"] unsignedLongLongValue];
        if (curTimestamp > endStamp) {
            return nil;
        }
        NSMutableDictionary* result = [event mutableCopy];
        [result removeObjectForKey: @"dt"];
        result[@"t"] = decodeTimestamp(curTimestamp);
        return result;
    }];
}


@end



// Internal enumerator implementation whose -nextObject method just calls a block
@implementation CBLTimeSeriesEnumerator
{
    CBLJSONDict* (^_block)();
}

- (id) initWithBlock: (CBLJSONDict*(^)())block
{
    self = [super init];
    if (self) {
        _block = block;
    }
    return self;
}

- (CBLJSONDict*) nextObject {
    if (!_block)
        return nil;
    id result = _block();
    if (!result)
        _block = nil;
    return result;
}

@end
