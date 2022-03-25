package com.intel.cosbench.api.S3ZStor;

import com.intel.cosbench.api.storage.*;

public class S3ZStorageFactory implements StorageAPIFactory {

    @Override
    public String getStorageName() {
        return "s3z";
    }

    @Override
    public StorageAPI getStorageAPI() {
        return new S3ZStorage();
    }

}
