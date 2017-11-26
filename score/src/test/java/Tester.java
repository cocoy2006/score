package test.java;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

public class Tester {

	public static void main(String[] args) throws ClientProtocolException, IOException {
		String loginUrl = "http://www.wicloud.cc/wicloud2/totalinfo/comeinAndAround/";
        List<NameValuePair> params = new ArrayList<NameValuePair>();
        params.add(new BasicNameValuePair("place", "test01"));
        params.add(new BasicNameValuePair("time", "2016-07-17"));
             
        requestPost(loginUrl,params);
        
		System.out.println("Done.");
		System.exit(0);
	}
	
	public static void requestPost(String url,List<NameValuePair> params) throws ClientProtocolException, IOException {
	    CloseableHttpClient httpclient = HttpClientBuilder.create().build();
	         
	    HttpPost httppost = new HttpPost(url);
	        httppost.setEntity(new UrlEncodedFormEntity(params));
	         
	        CloseableHttpResponse response = httpclient.execute(httppost);
	        System.out.println(response.toString());
	         
	        HttpEntity entity = response.getEntity();
	        String jsonStr = EntityUtils.toString(entity, "utf-8");
	        System.out.println(jsonStr);
	         
	        httppost.releaseConnection();
	}

}
