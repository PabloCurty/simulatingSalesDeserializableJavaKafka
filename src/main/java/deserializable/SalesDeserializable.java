package deserializable;

import com.fasterxml.jackson.databind.ObjectMapper;
import model.Venda;
import org.apache.kafka.common.serialization.Deserializer;


public class SalesDeserializable implements Deserializer<Venda> {
    @Override
    public Venda deserialize(String s, byte[] data) {
        try {
            return new ObjectMapper().readValue(data, Venda.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
