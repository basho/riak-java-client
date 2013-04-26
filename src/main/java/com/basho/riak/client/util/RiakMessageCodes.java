/*
 * Copyright 2013 Basho Technologies Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.basho.riak.client.util;

/**
 * PBC API message code constants.
 * @since 1.0
 */
public interface RiakMessageCodes 
{
	public static final byte MSG_ErrorResp = 0;
	public static final byte MSG_PingReq = 1;
	public static final byte MSG_PingResp = 2;
	public static final byte MSG_GetClientIdReq = 3;
	public static final byte MSG_GetClientIdResp = 4;
	public static final byte MSG_SetClientIdReq = 5;
	public static final byte MSG_SetClientIdResp = 6;
	public static final byte MSG_GetServerInfoReq = 7;
	public static final byte MSG_GetServerInfoResp = 8;
	public static final byte MSG_GetReq = 9;
	public static final byte MSG_GetResp = 10;
	public static final byte MSG_PutReq = 11;
	public static final byte MSG_PutResp = 12;
	public static final byte MSG_DelReq = 13;
	public static final byte MSG_DelResp = 14;
	public static final byte MSG_ListBucketsReq = 15;
	public static final byte MSG_ListBucketsResp = 16;
	public static final byte MSG_ListKeysReq = 17;
	public static final byte MSG_ListKeysResp = 18;
	public static final byte MSG_GetBucketReq = 19;
	public static final byte MSG_GetBucketResp = 20;
	public static final byte MSG_SetBucketReq = 21;
	public static final byte MSG_SetBucketResp = 22;
	public static final byte MSG_MapRedReq = 23;
	public static final byte MSG_MapRedResp = 24;
	public static final byte MSG_IndexReq = 25;
	public static final byte MSG_IndexResp = 26;
}
