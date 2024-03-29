package ooogenerator.configModel;

import java.util.List;

/**
 * Created by Philipp Grulich.
 */
public class DataGeneratorConfig {
	public String outputFilePath;
	public String rawFilePath;
	public Integer keyIndex;
	public String keySelect;
	public Integer timeIndex;
	public String srcTimeScale;
	public Long startTime;
	public Long endTime;
	public String seperator;
	public List<DataGeneratorExperimentConfig> generatorConfigurations;

}
