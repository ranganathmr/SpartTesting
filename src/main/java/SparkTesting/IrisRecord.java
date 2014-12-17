package SparkTesting;

import java.io.Serializable;
import java.util.Map;

import com.google.common.collect.Maps;

public class IrisRecord implements Serializable {
	
	private static Map<String, Double> irisMap = Maps.newHashMap();
	
	private static final long serialVersionUID = 1L;
	
	static {
		irisMap.put("Iris-virginica", 3.0);
		irisMap.put("Iris-versicolor", 2.0);
		irisMap.put("Iris-setosa", 1.0);
	}
	
	private Double petalLength;
	private Double petalWidth;
	private Double sepalLength;
	private Double sepalWidth;
	private Double species;
	
	public IrisRecord(String data) {
		String[] parts = data.split(",");
		this.petalLength = Double.valueOf(parts[2]);
		this.petalWidth = Double.valueOf(parts[3]);
		this.sepalLength = Double.valueOf(parts[0]);
		this.sepalWidth = Double.valueOf(parts[1]);
		this.species = irisMap.get(parts[4]);
	}

	public Double getPetalLength() {
		return petalLength;
	}

	public Double getPetalWidth() {
		return petalWidth;
	}

	public Double getSepalLength() {
		return sepalLength;
	}

	public Double getSepalWidth() {
		return sepalWidth;
	}

	public Double getSpecies() {
		return species;
	}

	@Override
	public String toString() {
		return sepalLength.toString();
	}
	
}