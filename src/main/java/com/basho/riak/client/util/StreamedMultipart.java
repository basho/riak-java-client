package com.basho.riak.client.util;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;

import com.basho.riak.client.util.Multipart.Part;

public class StreamedMultipart implements Iterator<Part> {

    Map<String, String> headers = null;
    BranchableInputStream stream;
    String boundary;
    boolean foundNext = false;
    OneTokenInputStream currentPartStream = null;

    /**
     * Parses a multipart message or a multipart subpart of a multipart message.
     * Each parts of the message is parsed into a map of headers and the body as
     * an InputStream. stream is not consumed until the return value is iterated
     * over. It is consumed as each part is encountered. A part's body
     * InputStream does not need to be consumed before proceed to the next part.
     * If it is not consumed, it will be buffered in memory and accessible
     * later.
     * 
     * @param headers
     *            The headers from the original message, which contains the
     *            Content-Type header including the boundary string
     * @param stream
     *            The input stream to read from
     * @throws IOException 
     */
    public StreamedMultipart(Map<String, String> headers, InputStream stream) throws IOException, EOFException {
        if (headers == null || stream == null)
            throw new IllegalArgumentException();

        String initialBoundary = "--" + Multipart.getBoundary(headers.get(Constants.HDR_CONTENT_TYPE));
        String boundary = "\n" + initialBoundary;

        // Find the first boundary, ignoring everything preceding it
        StringBuilder sb = new StringBuilder();
        while (true) {
            int c = stream.read();
            if (c == -1)
                throw new EOFException();
            sb.append(c);
            if ((sb.length() == initialBoundary.length() && initialBoundary.equals(sb.toString())) ||
                    (sb.indexOf(boundary, sb.length() - boundary.length()) >= 0))
                break;
        }

        this.headers = headers;
        this.boundary = boundary;
        this.stream = new BranchableInputStream(new OneTokenInputStream(stream, boundary + "--"));
    }

    /** 
     * Return the map of document headers that this object was constructed with.
     */
    public Map<String, String> getHeaders() {
        return headers;
    }

    /**
     * See {@link Iterator#hasNext()}.
     * 
     * @throws RuntimeException
     *             (IOException) if there is an error reading the next part from
     *             the input stream
     */
    public boolean hasNext() {
        if (foundNext)
            return true;

        try {
            foundNext = findNext();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return foundNext;
    }

    /**
     * See {@link Iterator#next()}.
     * 
     * @throws RuntimeException
     *             (IOException) if there is an error reading the next part from
     *             the input stream
     */
    public Part next() {
        if (!hasNext())
            return null;
        foundNext = false;

        String headerBlock = null;
        try {
            headerBlock = readHeaderBlock(currentPartStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Map<String, String> headers = Multipart.parseHeaders(headerBlock);
        return new Part(headers, currentPartStream);
    }

    public void remove() { /* nop */}

    /**
     * Move this.currentPartStream to the next part in the entity
     * 
     * @return true if there is another part was found
     * 
     * @throws IOException
     */
    private boolean findNext() throws IOException {
        if (currentPartStream != null && !currentPartStream.done()) {
            InputStream is = new OneTokenInputStream(stream, boundary);
            try {
                while (is.read() != -1) { /* nop */}
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        if (stream.peek() != -1) {
            currentPartStream = new OneTokenInputStream(stream.branch(), boundary);
            return true;
        }
        return false;
    }

    /**
     * Read in the header block from a stream (i.e. everything before and
     * including the first empty line)
     * 
     * @param in
     *            Stream to read header block from
     * 
     * @return String containing the header block
     */
    private String readHeaderBlock(InputStream in) throws IOException {
        StringBuilder headers = new StringBuilder();
        boolean currentLineEmpty = true;
        while (true) {
            int c = in.read();
            if (c == -1 || (currentLineEmpty && c == '\n')) {
                break;
            } else if (c == '\n') {
                currentLineEmpty = true;
            } else if (c != '\r' && c != '\n') {
                currentLineEmpty = false;
            }
            headers.append((char) c);
        }
        return headers.toString();
    }
}