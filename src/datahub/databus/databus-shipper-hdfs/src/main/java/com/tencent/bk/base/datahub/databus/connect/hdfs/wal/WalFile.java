/*
 * Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础平台 available.
 *
 * Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
 *
 * BK-BASE 蓝鲸基础平台 is licensed under the MIT License.
 *
 * License for BK-BASE 蓝鲸基础平台:
 * --------------------------------------------------------------------
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
 * and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial
 * portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
 * LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
 * NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.tencent.bk.base.datahub.databus.connect.hdfs.wal;

import com.tencent.bk.base.datahub.databus.connect.hdfs.HdfsConsts;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;

import org.apache.commons.codec.CharEncoding;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.VersionMismatchException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.Options;
import org.apache.hadoop.util.Time;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.rmi.server.UID;
import java.security.MessageDigest;
import java.util.Arrays;

public class WalFile {

    private static final Logger log = LoggerFactory.getLogger(WalFile.class);
    private static final byte INITIAL_VERSION = (byte) 0;
    private static byte[] VERSION = new byte[]{
            (byte) 'W', (byte) 'A', (byte) 'L', INITIAL_VERSION
    };

    private static final int SYNC_ESCAPE = -1;      // "length" of sync entries
    private static final int SYNC_HASH_SIZE = 16;   // number of bytes in hash
    private static final int SYNC_SIZE = 4 + SYNC_HASH_SIZE; // escape + hash

    /**
     * The number of bytes between sync points.
     */
    public static final int SYNC_INTERVAL = 100 * SYNC_SIZE;

    private WalFile() {
    }

    /**
     * 创建writer
     *
     * @param conf hdfs配置
     * @param opts writer参数
     * @return writer对象
     * @throws IOException 异常
     */
    public static Writer createWriter(Configuration conf, Writer.Option... opts) throws IOException {
        return new Writer(conf, opts);
    }


    public static class Writer implements Closeable, Syncable {

        private FSDataOutputStream out;
        private DataOutputBuffer buffer = new DataOutputBuffer();
        boolean ownOutputStream = true;
        private boolean appendMode;
        protected Serializer keySerializer;
        protected Serializer valSerializer;

        // Insert a globally unique 16-byte value every few entries, so that one
        // can seek into the middle of a file and then synchronize with record
        // starts and ends by scanning for this value.
        long lastSyncPos;                     // position of last sync
        byte[] sync;                          // 16 random bytes

        {
            try {
                MessageDigest digester = MessageDigest.getInstance("MD5");
                long time = Time.now();
                digester.update((new UID() + "@" + time).getBytes(Charset.forName(CharEncoding.UTF_8)));
                sync = digester.digest();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * A tag interface for all of the Reader options
         */
        public interface Option {

        }

        public static Option file(Path value) {
            return new FileOption(value);
        }

        public static Option appendIfExists(boolean value) {
            return new AppendIfExistsOption(value);
        }

        static class FileOption extends Options.PathOption implements Option {

            FileOption(Path path) {
                super(path);
            }
        }

        static class AppendIfExistsOption extends Options.BooleanOption implements Option {

            AppendIfExistsOption(boolean value) {
                super(value);
            }
        }


        Writer(Configuration conf, Option... opts) throws IOException {
            // 删除部分参数,仅仅留下本项目中有用的参数
            FileOption fileOption = Options.getOption(FileOption.class, opts);
            AppendIfExistsOption appendIfExistsOption = Options.getOption(AppendIfExistsOption.class, opts);

            // check consistency of options
            if (fileOption == null) {
                throw new IllegalArgumentException("file must be specified");
            }

            FileSystem fs = null;
            FSDataOutputStream out;
            Path p = fileOption.getValue();
            try {
                fs = p.getFileSystem(conf);
                int bufferSize = getBufferSize(conf);
                short replication = fs.getDefaultReplication(p);
                long blockSize = fs.getDefaultBlockSize(p);

                if (appendIfExistsOption != null && appendIfExistsOption.getValue() && fs.exists(p)) {
                    // Read the file and verify header details
                    try (WalFile.Reader reader = new WalFile.Reader(conf, WalFile.Reader.file(p),
                            new Reader.OnlyHeaderOption())) {
                        if (reader.getVersion() != VERSION[3]) {
                            throw new VersionMismatchException(VERSION[3], reader.getVersion());
                        }
                        sync = reader.getSync();
                    }
                    out = fs.append(p, bufferSize);
                    this.appendMode = true;
                } else {
                    out = fs.create(p, true, bufferSize, replication, blockSize);
                }

                init(conf, out, true);
            } catch (RemoteException re) {
                LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.HDFS_ERR,
                        String.format("Failed creating a WAL Writer for %s", p), re);
                if (re.getClassName().equals(HdfsConsts.LEASE_EXCEPTION)) {
                    LogUtils.info(log, "Lease is hold by another thread! {}", p);
                    throw new ConnectException("Lease is hold by another thread! " + p.toString(), re);
                }
                throw re;
            }
        }

        void init(Configuration conf, FSDataOutputStream out, boolean ownStream) throws IOException {
            this.out = out;
            this.ownOutputStream = ownStream;
            SerializationFactory serializationFactory = new SerializationFactory(conf);
            this.keySerializer = serializationFactory.getSerializer(WalEntry.class);
            if (this.keySerializer == null) {
                throw new IOException("Could not find a serializer for the Key class: '"
                        + WalEntry.class.getCanonicalName() + "'. Please ensure that the configuration '"
                        + CommonConfigurationKeys.IO_SERIALIZATIONS_KEY
                        + "' is properly configured, if you're using custom serialization.");
            }
            this.keySerializer.open(buffer);
            this.valSerializer = serializationFactory.getSerializer(WalEntry.class);
            if (this.valSerializer == null) {
                throw new IOException("Could not find a serializer for the Value class: '"
                        + WalEntry.class.getCanonicalName() + "'. Please ensure that the configuration '"
                        + CommonConfigurationKeys.IO_SERIALIZATIONS_KEY
                        + "' is properly configured, if you're using custom serialization.");
            }
            this.valSerializer.open(buffer);
            if (appendMode) {
                sync();
            } else {
                writeFileHeader();
            }
        }

        public void append(Writable key, Writable val)
                throws IOException {
            append((Object) key, (Object) val);
        }

        @SuppressWarnings("unchecked")
        private synchronized void append(Object key, Object val) throws IOException {
            buffer.reset();

            // Append the 'key'
            keySerializer.serialize(key);
            int keyLength = buffer.getLength();
            if (keyLength < 0) {
                throw new IOException("negative length keys not allowed: " + key);
            }

            valSerializer.serialize(val);

            // Write the record out
            checkAndWriteSync();                                // sync
            out.writeInt(buffer.getLength());                   // total record length
            out.writeInt(keyLength);                            // key portion length
            out.write(buffer.getData(), 0, buffer.getLength()); // data
        }

        /**
         * Returns the current length of the output file.
         * This always returns a synchronized position.  In other words,
         * immediately after calling {@link WalFile.Reader#seek(long)} with a position
         * returned by this method, {@link WalFile.Reader#next(Writable)} may be called.  However
         * the key may be earlier in the file than key last written when this
         * method was called (e.g., with block-compression, it may be the first key
         * in the block that was being written when this method was called).
         */
        public synchronized long getLength() throws IOException {
            return out.getPos();
        }

        private synchronized void checkAndWriteSync() throws IOException {
            if (sync != null && out.getPos() >= lastSyncPos + SYNC_INTERVAL) { // time to emit sync
                sync();
            }
        }

        private void writeFileHeader() throws IOException {
            out.write(VERSION);                    // write the version
            out.write(sync);                       // write the sync bytes
            out.flush();                           // flush header
        }


        @Override
        public synchronized void close() throws IOException {
            keySerializer.close();
            valSerializer.close();
            if (out != null) {
                // Close the underlying stream iff we own it...
                if (ownOutputStream) {
                    out.close();
                } else {
                    out.flush();
                }
                out = null;
            }
        }

        @Override
        public void sync() throws IOException {
            if (sync != null && lastSyncPos != out.getPos()) {
                out.writeInt(SYNC_ESCAPE);                // mark the start of the sync
                out.write(sync);                          // write sync
                lastSyncPos = out.getPos();               // update lastSyncPos
            }
        }

        @Override
        public void hsync() throws IOException {
            if (out != null) {
                out.hsync();
            }
        }

        @Override
        public void hflush() throws IOException {
            if (out != null) {
                out.hflush();
            }
        }
    }

    /**
     * Get the configured buffer size
     */
    private static int getBufferSize(Configuration conf) {
        return conf.getInt("io.file.buffer.size", 4096);
    }

    public static class Reader implements java.io.Closeable {

        private String filename;
        private FSDataInputStream in;
        private DataOutputBuffer outBuf = new DataOutputBuffer();

        private byte version;
        private byte[] sync = new byte[SYNC_HASH_SIZE];
        private byte[] syncCheck = new byte[SYNC_HASH_SIZE];
        private boolean syncSeen;

        private long headerEnd;
        private long end;
        private int keyLength;

        private Configuration conf;

        private DataInputBuffer valBuffer = null;
        private DataInputStream valIn = null;
        private Deserializer keyDeserializer;
        private Deserializer valDeserializer;

        /**
         * A tag interface for all of the Reader options
         */
        public interface Option {

        }

        /**
         * Create an option to specify the path name of the sequence file.
         *
         * @param value the path to read
         * @return a new option
         */
        public static Option file(Path value) {
            return new FileOption(value);
        }

        private static class FileOption extends Options.PathOption
                implements Option {

            private FileOption(Path value) {
                super(value);
            }
        }

        // only used directly
        private static class OnlyHeaderOption extends Options.BooleanOption
                implements Option {

            private OnlyHeaderOption() {
                super(true);
            }
        }

        public Reader(Configuration conf, Option... opts) throws IOException {
            // Look up the options, these are null if not set
            FileOption fileOpt = Options.getOption(FileOption.class, opts);
            // check for consistency
            if ((fileOpt == null)) {
                throw new IllegalArgumentException("File option must be specified");
            }
            // figure out the real values
            Path filename = fileOpt.getValue();
            FSDataInputStream file;
            final long len;
            FileSystem fs = null;

            try {
                fs = filename.getFileSystem(conf);
                int bufSize = getBufferSize(conf);
                len = fs.getFileStatus(filename).getLen();
                file = openFile(fs, filename, bufSize, len);

                OnlyHeaderOption headerOnly = Options.getOption(OnlyHeaderOption.class, opts);
                // really set up
                initialize(filename, file, 0, len, conf, headerOnly != null);
            } catch (RemoteException re) {
                LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.HDFS_ERR,
                        String.format("Failed creating a WAL Reader for %s", filename), re);
                if (re.getClassName().equals(HdfsConsts.LEASE_EXCEPTION)) {
                    LogUtils.warn(log, "Lease is hold by another thread! {}", filename);
                    throw new ConnectException("Lease is hold by another thread! " + filename, re);
                }
                throw re;
            }
        }

        /**
         * Common work of the constructors.
         */
        private void initialize(Path filename, FSDataInputStream in,
                long start, long length, Configuration conf,
                boolean tempReader) throws IOException {
            if (in == null) {
                throw new IllegalArgumentException("in == null");
            }
            this.filename = filename == null ? "<unknown>" : filename.toString();
            this.in = in;
            this.conf = conf;
            boolean succeeded = false;
            try {
                seek(start);
                this.end = this.in.getPos() + length;
                // if it wrapped around, use the max
                if (end < length) {
                    end = Long.MAX_VALUE;
                }
                init(tempReader);
                succeeded = true;
            } finally {
                if (!succeeded) {
                    if (this.in != null) {
                        try {
                            this.in.close();
                        } catch (IOException ignore) {
                            // ignore
                        }
                    }
                }
            }
        }

        /**
         * Override this method to specialize the type of {@link FSDataInputStream} returned.
         *
         * @param fs The file system used to open the file.
         * @param file The file being read.
         * @param bufferSize The buffer size used to read the file.
         * @param length The length being read if it is >= 0.  Otherwise, the length is not
         *         available.
         * @return The opened stream.
         */
        protected FSDataInputStream openFile(FileSystem fs, Path file, int bufferSize, long length) throws IOException {
            return fs.open(file, bufferSize);
        }

        /**
         * Initialize the {@link Reader}
         *
         * @param tempReader <code>true</code> if we are constructing a temporary and hence do not
         *         initialize every component; <code>false</code> otherwise.
         */
        private void init(boolean tempReader) throws IOException {
            byte[] versionBlock = new byte[VERSION.length];
            in.readFully(versionBlock);

            if ((versionBlock[0] != VERSION[0]) || (versionBlock[1] != VERSION[1]) || (versionBlock[2] != VERSION[2])) {
                throw new IOException(this + " not a WalFile");
            }

            // Set 'version'
            version = versionBlock[3];
            if (version > VERSION[3]) {
                throw new VersionMismatchException(VERSION[3], version);
            }

            in.readFully(sync);                       // read sync bytes
            headerEnd = in.getPos();                  // record end of header

            // Initialize... *not* if this we are constructing a temporary Reader
            if (!tempReader) {
                valBuffer = new DataInputBuffer();
                valIn = valBuffer;

                SerializationFactory serializationFactory = new SerializationFactory(conf);
                this.keyDeserializer = getDeserializer(serializationFactory, WalEntry.class);
                if (this.keyDeserializer == null) {
                    throw new IOException("Could not find a deserializer for the Key class: '"
                            + WalFile.class.getCanonicalName() + "'. Please ensure that the configuration '"
                            + CommonConfigurationKeys.IO_SERIALIZATIONS_KEY
                            + "' is properly configured, if you're using custom serialization.");
                }

                this.keyDeserializer.open(valBuffer);

                this.valDeserializer = getDeserializer(serializationFactory, WalEntry.class);
                if (this.valDeserializer == null) {
                    throw new IOException("Could not find a deserializer for the Value class: '"
                            + WalEntry.class.getCanonicalName() + "'. Please ensure that the configuration '"
                            + CommonConfigurationKeys.IO_SERIALIZATIONS_KEY
                            + "' is properly configured, if you're using custom serialization.");
                }
                this.valDeserializer.open(valIn);
            }
        }

        @SuppressWarnings("unchecked")
        private Deserializer getDeserializer(SerializationFactory sf, Class c) {
            return sf.getDeserializer(c);
        }

        /**
         * Close the file.
         */
        @Override
        public synchronized void close() throws IOException {
            if (keyDeserializer != null) {
                keyDeserializer.close();
            }
            if (valDeserializer != null) {
                valDeserializer.close();
            }

            // Close the input-stream
            in.close();
        }

        private synchronized byte[] getSync() {
            return sync;
        }

        private byte getVersion() {
            return version;
        }

        /**
         * Returns the configuration used for this file.
         */
        Configuration getConf() {
            return conf;
        }


        /**
         * Position valLenIn/valIn to the 'value' corresponding to the 'current' key
         */
        private synchronized void seekToCurrentValue() throws IOException {
            valBuffer.reset();
        }

        /**
         * Get the 'value' corresponding to the last read 'key'.
         *
         * @param val : The 'value' to be read.
         */
        public synchronized void getCurrentValue(Writable val)
                throws IOException {
            if (val instanceof Configurable) {
                ((Configurable) val).setConf(this.conf);
            }
            // Position stream to 'current' value
            seekToCurrentValue();

            val.readFields(valIn);
            if (valIn.read() > 0) {
                LogUtils.info(log, "available bytes: " + valIn.available());
                throw new IOException(val + " read " + (valBuffer.getPosition() - keyLength)
                        + " bytes, should read " + (valBuffer.getLength() - keyLength));
            }
        }

        /**
         * Get the 'value' corresponding to the last read 'key'.
         *
         * @param val : The 'value' to be read.
         */
        public synchronized Object getCurrentValue(Object val) throws IOException {
            if (val instanceof Configurable) {
                ((Configurable) val).setConf(this.conf);
            }

            // Position stream to 'current' value
            seekToCurrentValue();
            val = deserializeValue(val);
            if (valIn.read() > 0) {
                LogUtils.info(log, "available bytes: " + valIn.available());
                throw new IOException(val + " read " + (valBuffer.getPosition() - keyLength)
                        + " bytes, should read " + (valBuffer.getLength() - keyLength));
            }
            return val;

        }

        @SuppressWarnings("unchecked")
        private Object deserializeValue(Object val) throws IOException {
            return valDeserializer.deserialize(val);
        }

        /**
         * Read the next key in the file into <code>key</code>, skipping its value.  True if another
         * entry exists, and false at end of file.
         */
        public synchronized boolean next(Writable key) throws IOException {
            if (key.getClass() != WalEntry.class) {
                throw new IOException("wrong key class: " + key.getClass().getName() + " is not " + WalEntry.class);
            }

            outBuf.reset();

            keyLength = next(outBuf);
            if (keyLength < 0) {
                return false;
            }

            valBuffer.reset(outBuf.getData(), outBuf.getLength());

            key.readFields(valBuffer);
            valBuffer.mark(0);
            if (valBuffer.getPosition() != keyLength) {
                throw new IOException(key + " read " + valBuffer.getPosition() + " bytes, should read " + keyLength);
            }

            return true;
        }

        /**
         * Read the next key/value pair in the file into <code>key</code> and <code>val</code>.  Returns
         * true if such a pair exists and false when at end of file
         */
        public synchronized boolean next(Writable key, Writable val) throws IOException {
            if (val.getClass() != WalEntry.class) {
                throw new IOException("wrong value class: " + val + " is not " + WalEntry.class);
            }

            boolean more = next(key);

            if (more) {
                getCurrentValue(val);
            }

            return more;
        }

        /**
         * Read the next key/value pair in the file into <code>buffer</code>.
         * Returns the length of the key read, or -1 if at end of file.  The length
         * of the value may be computed by calling buffer.getLength() before and
         * after calls to this method.
         */
        synchronized int next(DataOutputBuffer buffer) throws IOException {
            try {
                int length = readRecordLength();
                if (length == -1) {
                    return -1;
                }
                int keyLength = in.readInt();
                buffer.write(in, length);
                return keyLength;
            } catch (ChecksumException e) {             // checksum failure
                handleChecksumException(e);
                return next(buffer);
            }
        }

        /**
         * Read the next key in the file, skipping its value.  Return null at end of file.
         */
        public synchronized Object next(Object key) throws IOException {
            if (key != null && key.getClass() != WalEntry.class) {
                throw new IOException("wrong key class: " + key.getClass().getName() + " is not " + WalEntry.class);
            }

            outBuf.reset();
            keyLength = next(outBuf);
            if (keyLength < 0) {
                return null;
            }
            valBuffer.reset(outBuf.getData(), outBuf.getLength());
            key = deserializeKey(key);
            valBuffer.mark(0);
            if (valBuffer.getPosition() != keyLength) {
                throw new IOException(key + " read " + valBuffer.getPosition() + " bytes, should read " + keyLength);
            }
            return key;
        }

        /**
         * Read and return the next record length, potentially skipping over a sync block.
         *
         * @return the length of the next record or -1 if there is no next record
         */
        private synchronized int readRecordLength() throws IOException {
            if (in.getPos() >= end) {
                return -1;
            }
            int length = in.readInt();
            if (sync != null && length == SYNC_ESCAPE) {              // process a sync entry
                in.readFully(syncCheck);                // read syncCheck
                if (!Arrays.equals(sync, syncCheck)) {  // check it
                    throw new IOException("File is corrupt!");
                }
                syncSeen = true;
                if (in.getPos() >= end) {
                    return -1;
                }
                length = in.readInt();                  // re-read length
            } else {
                syncSeen = false;
            }

            return length;
        }

        @SuppressWarnings("unchecked")
        private Object deserializeKey(Object key) throws IOException {
            return keyDeserializer.deserialize(key);
        }

        private void handleChecksumException(ChecksumException e) throws IOException {
            if (this.conf.getBoolean("io.skip.checksum.errors", false)) {
                LogUtils.warn(log, "Bad checksum at " + getPosition() + ". Skipping entries.");
                sync(getPosition() + this.conf.getInt("io.bytes.per.checksum", 512));
            } else {
                throw e;
            }
        }

        /**
         * disables sync. often invoked for tmp files
         */
        synchronized void ignoreSync() {
            sync = null;
        }

        /**
         * Set the current byte position in the input file.
         * The position passed must be a position returned by {@link WalFile.Writer#getLength()}
         * when writing this file.  To seek to an arbitrary position, use {@link
         * WalFile.Reader#sync(long)}.
         */
        public synchronized void seek(long position) throws IOException {
            in.seek(position);
        }

        /**
         * Seek to the next sync mark past a given position.
         */
        public synchronized void sync(long position) throws IOException {
            if (position + SYNC_SIZE >= end) {
                seek(end);
                return;
            }

            if (position < headerEnd) {
                // seek directly to first record
                in.seek(headerEnd);
                // note the sync marker "seen" in the header
                syncSeen = true;
                return;
            }

            try {
                seek(position + 4);                         // skip escape
                in.readFully(syncCheck);
                int syncLen = sync.length;
                for (int i = 0; in.getPos() < end; i++) {
                    int j = 0;
                    for (; j < syncLen; j++) {
                        if (sync[j] != syncCheck[(i + j) % syncLen]) {
                            break;
                        }
                    }
                    if (j == syncLen) {
                        in.seek(in.getPos() - SYNC_SIZE);     // position before sync
                        return;
                    }
                    syncCheck[i % syncLen] = in.readByte();
                }
            } catch (ChecksumException e) {             // checksum failure
                handleChecksumException(e);
            }
        }

        /**
         * Returns true iff the previous call to next passed a sync mark.
         */
        public synchronized boolean syncSeen() {
            return syncSeen;
        }

        /**
         * Return the current byte position in the input file.
         */
        public synchronized long getPosition() throws IOException {
            return in.getPos();
        }

        /**
         * Returns the name of the file.
         */
        @Override
        public String toString() {
            return filename;
        }
    }
}
