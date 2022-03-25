package com.intel.cosbench.api.S3V2Stor;

import com.intel.cosbench.api.storage.*;

public class S3V2StorageFactory implements StorageAPIFactory {

    @Override
    public String getStorageName() {
        return "s3z";
    }

    @Override
    public StorageAPI getStorageAPI() {
        return new S3V2Storage();
    }

}
