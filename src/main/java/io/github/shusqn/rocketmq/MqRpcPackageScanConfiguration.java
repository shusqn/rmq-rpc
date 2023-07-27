package io.github.shusqn.rocketmq;
import org.springframework.boot.autoconfigure.AutoConfigurationExcludeFilter;
import org.springframework.boot.context.TypeExcludeFilter;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.ComponentScan.Filter;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
 
@Configuration(
    proxyBeanMethods = false
)

//main @Import({MqRpcPackageScanConfiguration.class})
@ComponentScan(
    basePackages = {"com.frame.rocketmq"},
    excludeFilters = {@Filter(
    type = FilterType.CUSTOM,
    classes = {TypeExcludeFilter.class}
), @Filter(
    type = FilterType.CUSTOM,
    classes = {TypeExcludeFilter.class}
), @Filter(
    type = FilterType.CUSTOM,
    classes = {AutoConfigurationExcludeFilter.class}
)}
)
public class MqRpcPackageScanConfiguration {
    public MqRpcPackageScanConfiguration() {
    }
}