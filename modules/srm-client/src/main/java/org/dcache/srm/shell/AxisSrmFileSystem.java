/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2014 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.srm.shell;

import org.apache.axis.types.URI;
import org.apache.axis.types.UnsignedLong;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

import java.rmi.RemoteException;
import java.util.concurrent.TimeUnit;

import org.dcache.srm.SRMAbortedException;
import org.dcache.srm.SRMAuthorizationException;
import org.dcache.srm.SRMDuplicationException;
import org.dcache.srm.SRMExceedAllocationException;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMFileUnvailableException;
import org.dcache.srm.SRMInternalErrorException;
import org.dcache.srm.SRMInvalidPathException;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMNoFreeSpaceException;
import org.dcache.srm.SRMNonEmptyDirectoryException;
import org.dcache.srm.SRMNotSupportedException;
import org.dcache.srm.SRMOtherException;
import org.dcache.srm.SRMReleasedException;
import org.dcache.srm.SRMRequestTimedOutException;
import org.dcache.srm.SRMSpaceLifetimeExpiredException;
import org.dcache.srm.SRMTooManyResultsException;
import org.dcache.srm.v2_2.ArrayOfAnyURI;
import org.dcache.srm.v2_2.ArrayOfString;
import org.dcache.srm.v2_2.ArrayOfTGroupPermission;
import org.dcache.srm.v2_2.ArrayOfTUserPermission;
import org.dcache.srm.v2_2.ISRM;
import org.dcache.srm.v2_2.SrmCheckPermissionRequest;
import org.dcache.srm.v2_2.SrmCheckPermissionResponse;
import org.dcache.srm.v2_2.SrmGetPermissionRequest;
import org.dcache.srm.v2_2.SrmGetPermissionResponse;
import org.dcache.srm.v2_2.SrmGetSpaceMetaDataRequest;
import org.dcache.srm.v2_2.SrmGetSpaceMetaDataResponse;
import org.dcache.srm.v2_2.SrmGetSpaceTokensRequest;
import org.dcache.srm.v2_2.SrmGetSpaceTokensResponse;
import org.dcache.srm.v2_2.SrmGetTransferProtocolsRequest;
import org.dcache.srm.v2_2.SrmGetTransferProtocolsResponse;
import org.dcache.srm.v2_2.SrmLsRequest;
import org.dcache.srm.v2_2.SrmLsResponse;
import org.dcache.srm.v2_2.SrmMkdirRequest;
import org.dcache.srm.v2_2.SrmMkdirResponse;
import org.dcache.srm.v2_2.SrmMvRequest;
import org.dcache.srm.v2_2.SrmMvResponse;
import org.dcache.srm.v2_2.SrmPingRequest;
import org.dcache.srm.v2_2.SrmPingResponse;
import org.dcache.srm.v2_2.SrmReleaseSpaceRequest;
import org.dcache.srm.v2_2.SrmReleaseSpaceResponse;
import org.dcache.srm.v2_2.SrmReserveSpaceRequest;
import org.dcache.srm.v2_2.SrmReserveSpaceResponse;
import org.dcache.srm.v2_2.SrmRmRequest;
import org.dcache.srm.v2_2.SrmRmResponse;
import org.dcache.srm.v2_2.SrmRmdirRequest;
import org.dcache.srm.v2_2.SrmRmdirResponse;
import org.dcache.srm.v2_2.SrmStatusOfLsRequestRequest;
import org.dcache.srm.v2_2.SrmStatusOfLsRequestResponse;
import org.dcache.srm.v2_2.SrmStatusOfReserveSpaceRequestRequest;
import org.dcache.srm.v2_2.SrmStatusOfReserveSpaceRequestResponse;
import org.dcache.srm.v2_2.TAccessLatency;
import org.dcache.srm.v2_2.TFileType;
import org.dcache.srm.v2_2.TGroupPermission;
import org.dcache.srm.v2_2.TMetaDataPathDetail;
import org.dcache.srm.v2_2.TMetaDataSpace;
import org.dcache.srm.v2_2.TPermissionMode;
import org.dcache.srm.v2_2.TPermissionReturn;
import org.dcache.srm.v2_2.TRetentionPolicy;
import org.dcache.srm.v2_2.TRetentionPolicyInfo;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TSURLPermissionReturn;
import org.dcache.srm.v2_2.TStatusCode;
import org.dcache.srm.v2_2.TSupportedTransferProtocol;
import org.dcache.srm.v2_2.TUserPermission;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ObjectArrays.concat;
import static java.util.Arrays.asList;

@ParametersAreNonnullByDefault
public class AxisSrmFileSystem implements SrmFileSystem
{
    private final ISRM srm;

    public AxisSrmFileSystem(ISRM srm)
    {
        this.srm = srm;
    }

    @Nonnull
    @Override
    public TMetaDataPathDetail stat(URI surl) throws RemoteException, SRMException
    {
        SrmLsResponse response = srm.srmLs(
                new SrmLsRequest(null, new ArrayOfAnyURI(new URI[]{surl}), null, null, true, false, 0, 0, 1));
        if (response.getReturnStatus().getStatusCode() != TStatusCode.SRM_REQUEST_QUEUED &&
                response.getReturnStatus().getStatusCode() != TStatusCode.SRM_REQUEST_INPROGRESS) {
            checkSuccess(response.getReturnStatus(), TStatusCode.SRM_SUCCESS);
            return response.getDetails().getPathDetailArray(0);
        } else {
            SrmStatusOfLsRequestResponse status;
            do {
                status = srm.srmStatusOfLsRequest(
                        new SrmStatusOfLsRequestRequest(null, response.getRequestToken(), 0, 1));
            } while (response.getReturnStatus().getStatusCode() == TStatusCode.SRM_REQUEST_QUEUED ||
                    response.getReturnStatus().getStatusCode() == TStatusCode.SRM_REQUEST_INPROGRESS);
            checkSuccess(status.getReturnStatus(), TStatusCode.SRM_SUCCESS);
            return status.getDetails().getPathDetailArray(0);
        }
    }

    @Nonnull
    @Override
    public TPermissionMode checkPermission(URI surl) throws RemoteException, SRMException
    {
        TSURLPermissionReturn[] permission = checkPermissions(surl);
        checkSuccess(permission[0].getStatus(), TStatusCode.SRM_SUCCESS);
        return permission[0].getPermission();
    }

    @Nonnull
    @Override
    public TSURLPermissionReturn[] checkPermissions(URI... surls) throws RemoteException, SRMException
    {
        checkArgument(surls.length > 0);
        SrmCheckPermissionResponse response = srm.srmCheckPermission(
                new SrmCheckPermissionRequest(new ArrayOfAnyURI(surls), null, null));
        TSURLPermissionReturn[] permissionArray = response.getArrayOfPermissions().getSurlPermissionArray();
        if (permissionArray == null || permissionArray.length == 0) {
            checkSuccess(response.getReturnStatus(), TStatusCode.SRM_SUCCESS, TStatusCode.SRM_PARTIAL_SUCCESS);
            throw new SrmProtocolException("Server reply lacks permission array.");
        }

        checkSuccess(response.getReturnStatus(), TStatusCode.SRM_SUCCESS, TStatusCode.SRM_PARTIAL_SUCCESS,
                     TStatusCode.SRM_FAILURE);

        return response.getArrayOfPermissions().getSurlPermissionArray();
    }

    @Nonnull
    @Override
    public TPermissionReturn getPermission(URI surl) throws RemoteException, SRMException
    {
        TPermissionReturn[] permission = getPermissions(surl);
        checkSuccess(permission[0].getStatus(), TStatusCode.SRM_SUCCESS);
        return permission[0];
    }

    @Nonnull
    @Override
    public TPermissionReturn[] getPermissions(URI... surls) throws RemoteException, SRMException
    {
        checkArgument(surls.length > 0);
        SrmGetPermissionResponse response = srm.srmGetPermission(
                new SrmGetPermissionRequest(null, new ArrayOfAnyURI(surls), null));

        TPermissionReturn[] permissionArray = response.getArrayOfPermissionReturns().getPermissionArray();
        if (permissionArray == null || permissionArray.length == 0) {
            checkSuccess(response.getReturnStatus(), TStatusCode.SRM_SUCCESS, TStatusCode.SRM_PARTIAL_SUCCESS);
            throw new SrmProtocolException("Server reply lacks permission array.");
        }

        checkSuccess(response.getReturnStatus(), TStatusCode.SRM_SUCCESS, TStatusCode.SRM_PARTIAL_SUCCESS,
                     TStatusCode.SRM_FAILURE);

        // Simplify things for the caller
        for (TPermissionReturn permission: permissionArray) {
            if (permission.getArrayOfUserPermissions() == null) {
                permission.setArrayOfUserPermissions(new ArrayOfTUserPermission());
            }
            if (permission.getArrayOfUserPermissions().getUserPermissionArray() == null) {
                permission.getArrayOfUserPermissions().setUserPermissionArray(new TUserPermission[0]);
            }
            if (permission.getArrayOfGroupPermissions() == null) {
                permission.setArrayOfGroupPermissions(new ArrayOfTGroupPermission());
            }
            if (permission.getArrayOfGroupPermissions().getGroupPermissionArray() == null) {
                permission.getArrayOfGroupPermissions().setGroupPermissionArray(new TGroupPermission[0]);
            }
        }

        return permissionArray;
    }

    private TMetaDataPathDetail list(URI surl, boolean verbose, int offset,
                                     int count) throws RemoteException, SRMException, InterruptedException
    {
        SrmLsResponse response = srm.srmLs(
                new SrmLsRequest(null, new ArrayOfAnyURI(new URI[]{surl}), null, null, verbose, false, 1, offset,
                                 count));
        while (response.getReturnStatus().getStatusCode() == TStatusCode.SRM_REQUEST_QUEUED ||
                response.getReturnStatus().getStatusCode() == TStatusCode.SRM_REQUEST_INPROGRESS) {
            TimeUnit.SECONDS.sleep(2);
            SrmStatusOfLsRequestResponse status =
                    srm.srmStatusOfLsRequest(
                            new SrmStatusOfLsRequestRequest(null, response.getRequestToken(), offset, count));
            response.setDetails(status.getDetails());
            response.setReturnStatus(status.getReturnStatus());
        }
        checkSuccess(response.getReturnStatus(), TStatusCode.SRM_SUCCESS);
        return response.getDetails().getPathDetailArray()[0];
    }

    @Nonnull
    @Override
    public TMetaDataPathDetail[] list(URI surl, boolean verbose) throws RemoteException, SRMException, InterruptedException
    {
        int offset = 0;
        int count = 1000;
        TMetaDataPathDetail[] list = {};
        do {
            TMetaDataPathDetail detail = list(surl, verbose, offset, count);
            if (detail.getType() != TFileType.DIRECTORY) {
                throw new SRMInvalidPathException("Not a directory");
            }
            offset += count;
            TMetaDataPathDetail[] pathDetailArray = detail.getArrayOfSubPaths().getPathDetailArray();
            if (pathDetailArray != null) {
                list = concat(list, pathDetailArray, TMetaDataPathDetail.class);
            }
        } while (list.length == offset);
        return list;
    }

    @Nonnull
    @Override
    public SrmPingResponse ping() throws RemoteException, SRMException
    {
        return srm.srmPing(new SrmPingRequest());
    }

    @Nonnull
    @Override
    public TSupportedTransferProtocol[] getTransferProtocols() throws SRMException, RemoteException
    {
        SrmGetTransferProtocolsResponse response =
                srm.srmGetTransferProtocols(new SrmGetTransferProtocolsRequest());
        checkSuccess(response.getReturnStatus(), TStatusCode.SRM_SUCCESS);
        TSupportedTransferProtocol[] protocolArray = response.getProtocolInfo().getProtocolArray();
        return (protocolArray == null) ? new TSupportedTransferProtocol[0] : protocolArray;
    }

    @Override
    public void mkdir(URI surl) throws RemoteException, SRMException
    {
        SrmMkdirResponse response = srm.srmMkdir(new SrmMkdirRequest(null, surl, null));
        checkSuccess(response.getReturnStatus(), TStatusCode.SRM_SUCCESS);
    }

    @Override
    public void rmdir(URI lookup, boolean recursive) throws RemoteException, SRMException
    {
        SrmRmdirResponse response = srm.srmRmdir(new SrmRmdirRequest(null, lookup, null, recursive));
        checkSuccess(response.getReturnStatus(), TStatusCode.SRM_SUCCESS);
    }

    @Nonnull
    @Override
    public SrmRmResponse rm(URI... surls) throws RemoteException, SRMException
    {
        SrmRmResponse response = srm.srmRm(new SrmRmRequest(null, new ArrayOfAnyURI(surls), null));
        if (response.getArrayOfFileStatuses().getStatusArray() == null) {
            checkSuccess(response.getReturnStatus(), TStatusCode.SRM_SUCCESS);
        } else {
            checkSuccess(response.getReturnStatus(), TStatusCode.SRM_SUCCESS, TStatusCode.SRM_PARTIAL_SUCCESS,
                         TStatusCode.SRM_FAILURE);
        }
        return response;
    }

    @Override
    public void mv(URI fromSurl, URI toSurl) throws RemoteException, SRMException
    {
        SrmMvResponse response = srm.srmMv(new SrmMvRequest(null, fromSurl, toSurl, null));
        checkSuccess(response.getReturnStatus(), TStatusCode.SRM_SUCCESS);
    }

    @Nonnull
    @Override
    public String[] getSpaceTokens(String userSpaceTokenDescription) throws RemoteException, SRMException
    {
        SrmGetSpaceTokensResponse response = srm.srmGetSpaceTokens(
                new SrmGetSpaceTokensRequest(userSpaceTokenDescription, null));
        checkSuccess(response.getReturnStatus(), TStatusCode.SRM_SUCCESS);
        ArrayOfString arrayOfSpaceTokens = response.getArrayOfSpaceTokens();
        return (arrayOfSpaceTokens != null) ? arrayOfSpaceTokens.getStringArray() : new String[0];
    }

    @Nonnull
    @Override
    public TMetaDataSpace reserveSpace(long size, @Nullable String description, @Nullable TAccessLatency al,
                                       TRetentionPolicy rp, @Nullable Integer lifetime)
            throws SRMException, RemoteException, InterruptedException
    {
        SrmReserveSpaceResponse response = srm.srmReserveSpace(
                new SrmReserveSpaceRequest(null, description, new TRetentionPolicyInfo(rp, al), new UnsignedLong(size),
                                           new UnsignedLong(size),
                                           lifetime, null, null, null));
        while (response.getReturnStatus().getStatusCode() == TStatusCode.SRM_REQUEST_QUEUED ||
                response.getReturnStatus().getStatusCode() == TStatusCode.SRM_REQUEST_INPROGRESS) {
            if (response.getEstimatedProcessingTime() != null) {
                TimeUnit.SECONDS.sleep(response.getEstimatedProcessingTime());
            } else {
                TimeUnit.SECONDS.sleep(2);
            }
            SrmStatusOfReserveSpaceRequestResponse status =
                    srm.srmStatusOfReserveSpaceRequest(
                            new SrmStatusOfReserveSpaceRequestRequest(null, response.getRequestToken()));
            response.setReturnStatus(status.getReturnStatus());
            response.setLifetimeOfReservedSpace(status.getLifetimeOfReservedSpace());
            response.setRetentionPolicyInfo(status.getRetentionPolicyInfo());
            response.setSizeOfGuaranteedReservedSpace(status.getSizeOfGuaranteedReservedSpace());
            response.setSizeOfTotalReservedSpace(status.getSizeOfTotalReservedSpace());
            response.setSpaceToken(status.getSpaceToken());
            response.setEstimatedProcessingTime(status.getEstimatedProcessingTime());
        }
        checkSuccess(response.getReturnStatus(), TStatusCode.SRM_SUCCESS, TStatusCode.SRM_LOWER_SPACE_GRANTED);

        TMetaDataSpace space = new TMetaDataSpace();
        space.setLifetimeAssigned(response.getLifetimeOfReservedSpace());
        space.setRetentionPolicyInfo(response.getRetentionPolicyInfo());
        space.setGuaranteedSize(response.getSizeOfGuaranteedReservedSpace());
        space.setTotalSize(response.getSizeOfTotalReservedSpace());
        space.setSpaceToken(response.getSpaceToken());
        space.setStatus(response.getReturnStatus());
        return space;
    }

    @Override
    public void releaseSpace(String spaceToken) throws RemoteException, SRMException
    {
        SrmReleaseSpaceResponse response = srm.srmReleaseSpace(
                new SrmReleaseSpaceRequest(null, spaceToken, null, null));
        checkSuccess(response.getReturnStatus(), TStatusCode.SRM_SUCCESS);
    }

    @Nonnull
    @Override
    public TMetaDataSpace[] getSpaceMetaData(String... spaceTokens) throws RemoteException, SRMException
    {
        checkArgument(spaceTokens.length  > 0);
        SrmGetSpaceMetaDataResponse response = srm.srmGetSpaceMetaData(
                new SrmGetSpaceMetaDataRequest(null, new ArrayOfString(spaceTokens)));
        TMetaDataSpace[] spaceDataArray = response.getArrayOfSpaceDetails().getSpaceDataArray();
        if (spaceDataArray == null || spaceDataArray.length == 0) {
            checkSuccess(response.getReturnStatus(), TStatusCode.SRM_SUCCESS, TStatusCode.SRM_PARTIAL_SUCCESS);
            throw new SrmProtocolException("Server reply lacks space meta data.");
        } else {
            checkSuccess(response.getReturnStatus(), TStatusCode.SRM_SUCCESS, TStatusCode.SRM_PARTIAL_SUCCESS,
                         TStatusCode.SRM_FAILURE);
        }
        return spaceDataArray;
    }

    @Nonnull
    @Override
    public TMetaDataSpace getSpaceMetaData(String spaceToken) throws RemoteException, SRMException
    {
        TMetaDataSpace space = getSpaceMetaData(new String[]{spaceToken})[0];
        checkSuccess(space.getStatus(), TStatusCode.SRM_SUCCESS);
        return space;
    }

    private void checkSuccess(TReturnStatus returnStatus, TStatusCode... success) throws SRMException
    {
        TStatusCode statusCode = returnStatus.getStatusCode();
        String explanation = returnStatus.getExplanation();
        if (asList(success).contains(statusCode)) {
            return;
        }
        if (statusCode == TStatusCode.SRM_FAILURE) {
            throw new SRMException(explanation);
        } else if (statusCode == TStatusCode.SRM_PARTIAL_SUCCESS) {
            throw new SRMOtherException(statusCode, explanation);
        } else if (statusCode == TStatusCode.SRM_AUTHENTICATION_FAILURE) {
            throw new SRMOtherException(statusCode, explanation);
        } else if (statusCode == TStatusCode.SRM_AUTHORIZATION_FAILURE) {
            throw new SRMAuthorizationException(explanation);
        } else if (statusCode == TStatusCode.SRM_INVALID_REQUEST) {
            throw new SRMInvalidRequestException(explanation);
        } else if (statusCode == TStatusCode.SRM_INVALID_PATH) {
            throw new SRMInvalidPathException(explanation);
        } else if (statusCode == TStatusCode.SRM_FILE_LIFETIME_EXPIRED) {
            throw new SRMOtherException(statusCode, explanation);
        } else if (statusCode == TStatusCode.SRM_SPACE_LIFETIME_EXPIRED) {
            throw new SRMSpaceLifetimeExpiredException(explanation);
        } else if (statusCode == TStatusCode.SRM_EXCEED_ALLOCATION) {
            throw new SRMExceedAllocationException(explanation);
        } else if (statusCode == TStatusCode.SRM_NO_USER_SPACE) {
            throw new SRMOtherException(statusCode, explanation);
        } else if (statusCode == TStatusCode.SRM_NO_FREE_SPACE) {
            throw new SRMNoFreeSpaceException(explanation);
        } else if (statusCode == TStatusCode.SRM_DUPLICATION_ERROR) {
            throw new SRMDuplicationException(explanation);
        } else if (statusCode == TStatusCode.SRM_NON_EMPTY_DIRECTORY) {
            throw new SRMNonEmptyDirectoryException(explanation);
        } else if (statusCode == TStatusCode.SRM_TOO_MANY_RESULTS) {
            throw new SRMTooManyResultsException(explanation);
        } else if (statusCode == TStatusCode.SRM_INTERNAL_ERROR) {
            throw new SRMInternalErrorException(explanation);
        } else if (statusCode == TStatusCode.SRM_FATAL_INTERNAL_ERROR) {
            throw new SRMInternalErrorException(explanation);
        } else if (statusCode == TStatusCode.SRM_NOT_SUPPORTED) {
            throw new SRMNotSupportedException(explanation);
        } else if (statusCode == TStatusCode.SRM_REQUEST_QUEUED) {
            throw new SRMOtherException(statusCode, explanation);
        } else if (statusCode == TStatusCode.SRM_REQUEST_INPROGRESS) {
            throw new SRMOtherException(statusCode, explanation);
        } else if (statusCode == TStatusCode.SRM_ABORTED) {
            throw new SRMAbortedException(explanation);
        } else if (statusCode == TStatusCode.SRM_RELEASED) {
            throw new SRMReleasedException(explanation);
        } else if (statusCode == TStatusCode.SRM_FILE_PINNED) {
            throw new SRMOtherException(statusCode, explanation);
        } else if (statusCode == TStatusCode.SRM_FILE_IN_CACHE) {
            throw new SRMOtherException(statusCode, explanation);
        } else if (statusCode == TStatusCode.SRM_SPACE_AVAILABLE) {
            throw new SRMOtherException(statusCode, explanation);
        } else if (statusCode == TStatusCode.SRM_LOWER_SPACE_GRANTED) {
            throw new SRMOtherException(statusCode, explanation);
        } else if (statusCode == TStatusCode.SRM_REQUEST_TIMED_OUT) {
            throw new SRMRequestTimedOutException(explanation);
        } else if (statusCode == TStatusCode.SRM_LAST_COPY) {
            throw new SRMOtherException(statusCode, explanation);
        } else if (statusCode == TStatusCode.SRM_FILE_BUSY) {
            throw new SRMOtherException(statusCode, explanation);
        } else if (statusCode == TStatusCode.SRM_FILE_LOST) {
            throw new SRMOtherException(statusCode, explanation);
        } else if (statusCode == TStatusCode.SRM_FILE_UNAVAILABLE) {
            throw new SRMFileUnvailableException(explanation);
        } else if (statusCode == TStatusCode.SRM_CUSTOM_STATUS) {
            throw new SRMOtherException(statusCode, explanation);
        } else {
            throw new SRMOtherException(statusCode, explanation);
        }
    }
}
