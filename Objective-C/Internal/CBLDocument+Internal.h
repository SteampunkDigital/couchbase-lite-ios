//
//  CBLDocument+Internal.h
//  CouchbaseLite
//
//  Copyright (c) 2017 Couchbase, Inc All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

#pragma once
#import "CBLMutableArray.h"
#import "CBLBlob.h"
#import "CBLC4Document.h"
#import "CBLDatabase.h"
#import "CBLMutableDictionary.h"
#import "CBLMutableDocument.h"
#import "CBLDocumentFragment.h"
#import "CBLMutableFragment.h"
#import "CBLArray.h"
#import "CBLDocument.h"
#import "CBLDictionary.h"
#import "CBLFragment.h"
#import "Fleece.h"


NS_ASSUME_NONNULL_BEGIN

//////////////////

@interface CBLMutableDocument ()

- (instancetype) initAsCopyWithDocument: (CBLDocument*)doc
                                   dict: (nullable CBLDictionary*)dict;

- (void) setEncodingError: (NSError*)error;

/** For conflict resolver to know that the document is being deleted. */
- (void) markAsDeleted;

/** For making as invalidated. The invalidated document is not allowed
    to save or delete. */
- (void) markAsInvalidated;

@end

//////////////////

@interface CBLDocument ()
{
    @protected
    CBLDictionary* _dict;
    BOOL _isInvalidated;
}

@property (nonatomic, nullable) CBLDatabase* database;

@property (nonatomic, readonly) C4Database* c4db;   // Throws assertion-failure if null

@property (nonatomic, nullable) CBLC4Document* c4Doc;

@property (nonatomic, readonly) BOOL exists;

@property (nonatomic, readonly) BOOL isEmpty;

@property (nonatomic, readonly) NSString* revID;

@property (nonatomic, readonly) NSUInteger generation;

@property (nonatomic, readonly, nullable) FLDict fleeceData;

@property (nonatomic, readonly) BOOL isInvalidated;

- (instancetype) initWithDatabase: (nullable CBLDatabase*)database
                       documentID: (NSString*)documentID
                            c4Doc: (nullable CBLC4Document*)c4Doc NS_DESIGNATED_INITIALIZER;

- (instancetype) initWithDatabase: (CBLDatabase*)database
                       documentID: (NSString*)documentID
                   includeDeleted: (BOOL)includeDeleted
                            error: (NSError**)outError;

- (BOOL) selectConflictingRevision;
- (BOOL) selectCommonAncestorOfDoc: (CBLDocument*)doc1
                            andDoc: (CBLDocument*)doc2;

- (nullable NSData*) encode: (NSError**)outError;

@end

//////////////////

@interface CBLMutableDictionary ()

@property (readonly, nonatomic) BOOL changed;

@end

//////////////////

@interface CBLArray ()

- (instancetype) initEmpty;

@end

//////////////////

@interface CBLDictionary ()

- (instancetype) initEmpty;
- (void) keysChanged;
@end

/////////////////

@interface CBLFragment ()
{
    @protected
    id _parent;
    NSString* _key;           // Nil if this is an indexed subscript
    NSUInteger _index;        // Ignored if this is a keyed subscript
}

- (instancetype) initWithParent: (id)parent key: (NSString*)parentKey;
- (instancetype) initWithParent: (id)parent index: (NSUInteger)parentIndex;

@end

/////////////////

@interface CBLDocumentFragment ()

- (instancetype) initWithDocument: (CBLDocument*)document;

@end

NS_ASSUME_NONNULL_END
