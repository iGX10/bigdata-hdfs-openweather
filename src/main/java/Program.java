import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileReader;
import java.io.IOException;
import java.net.URI;

public class Program {
    public static void main(String[] args) throws IOException {

        // weather api
        String baseUrl = "http://api.openweathermap.org/data/2.5/forecast";
        String appid = "0d567ec04975432d3d2737fc52adf9dc";

        // hdfs
        String uri = "hdfs://localhost:9000/";
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "1");
        configuration.set("dfs.blocksize", "1m");
        FileSystem fs = FileSystem.get(URI.create(uri), configuration);

        String destinationDir = "/user/OpenWeatherForecast/";

        JSONParser jsonParser = new JSONParser();

        try (FileReader reader = new FileReader("src/main/resources/city.list.json")) {
            Object object = jsonParser.parse(reader);
            JSONArray cities = (JSONArray) object;
            //System.out.println(cities);
            cities.forEach(city -> {
                JSONObject c = (JSONObject) city;
                if(((String) c.get("country")).equals("MA")) {
                    //System.out.println(c.get("id"));
                    try {
                        HttpResponse<String> httpResponse = Unirest.get(
                                baseUrl+"?id="+c.get("id")+"&appid="+appid).asString();

                        //System.out.println( httpResponse.getBody());
                        JSONObject jsonResponse = (JSONObject) jsonParser.parse(httpResponse.getBody());
                        String forecastData = (String) jsonResponse.get("list").toString();
                        //System.out.println(forecastData);

                        FSDataOutputStream outputStream = fs.create(
                                new Path(destinationDir + c.get("id") + ".json"), new Progressable() {
                            @Override
                            public void progress() {
                                System.out.println("Creating files...");
                            }
                        });
                        outputStream.writeBytes(forecastData);

                        outputStream.close();

                    } catch (UnirestException | ParseException | IOException e) {
                        e.printStackTrace();
                    }
                }
            });
        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }
    }
}
