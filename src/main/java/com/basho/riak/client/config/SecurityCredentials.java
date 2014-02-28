/*
 * Copyright 2014 Brian Roach <roach at basho dot com>.
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

package com.basho.riak.client.config;

import java.io.File;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class SecurityCredentials
{
    private final KeyStore trustStore;
    private final String username;
    private final String password;
    
    private SecurityCredentials(Builder builder)
    {
        this.trustStore = builder.trustStore;
        this.username = builder.username;
        this.password = builder.password;
    }
    
    public KeyStore getTrustStore()
    {
        return trustStore;
    }
    
    public String getUsername()
    {
        return username;
    }
    
    public String getPassword()
    {
        return password;
    }
    
    
    public static class Builder
    {
        private String username;
        private String password;
        private KeyStore trustStore;
        
        /**
         * Construct a builder that produces a SecurityCredentials.
         */
        public Builder()
        {}
        
        /**
         * Construct a builder that produces a SecurityCredentials
         * @param username
         * @param password 
         */
        public Builder(String username, String password)
        {
            this.username = username;
            this.password = password;
        }
        
        /**
         * Set the Riak username.
         * @param username the name of the Riak user.
         * @return a reference to this object.
         */
        public Builder withUsername(String username)
        {
            this.username = username;
            return this;
        }
        
        /**
         * Set the Riak password.
         * @return a reference to this object.
         */
        public Builder withPassword(String password)
        {
            this.password = password;
            return this;
        }
        
        /**
         * Provide the CA certificate as a filename.
         * <p>
         * The certificate will be loaded from the file.
         * </p>
         * @param filename
         * @return a reference to this object.
         */
        public Builder withCACert(String filename, boolean isKeystore) 
        {
            File f = new File(filename);
            try
            {
                trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
                FileInputStream in = new FileInputStream(f);
                CertificateFactory cFactory = CertificateFactory.getInstance("X.509");
                X509Certificate caCert = (X509Certificate) cFactory.generateCertificate(in);
                in.close();

                trustStore.load(null, "password".toCharArray());
                trustStore.setCertificateEntry("mycert", caCert);
                return this;
            }
            catch (Exception e)
            {
                throw new SecurityCredentialsException("Could not create keystore from file", e);
            }
        }
        
        /**
         * Provide the CA certificate via a keystore file.
         * <p>
         * The keystore will be loaded and used as the trust manager 
         * for TLS/SSL.
         * </p>
         * @param keyStore the filename of the keystore.
         * @param password the password for the keystore.
         * @return a reference to this object.
         */
        public Builder withCACert(String keyStore, String password)
        {
            File f = new File(keyStore);
            try
            {
                FileInputStream in = new FileInputStream(f);
                trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
                trustStore.load(in, password.toCharArray());
                return this;
            }
            catch (Exception e)
            {
                throw new SecurityCredentialsException("Could not load keystore", e);
            }
        }
        
        public SecurityCredentials build()
        {
            if (username == null)
            {
                throw new IllegalStateException("Username cannot be null");
            }
            else if (password == null)
            {
                throw new IllegalStateException("Password cannot be null");
            }
            else if (trustStore == null)
            {
                throw new IllegalArgumentException("A CA certificate must be supplied for security");
            }
            else
            {
                return new SecurityCredentials(this);
            }
        }
    }
    
    public static class SecurityCredentialsException extends RuntimeException
    {
        public SecurityCredentialsException(String message, Throwable cause)
        {
            super(message, cause);
        }
    }
    
    
}
