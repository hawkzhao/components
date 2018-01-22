package org.talend.components.processing.definition.aggregate;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.talend.components.api.component.Connector;
import org.talend.components.api.component.PropertyPathConnector;
import org.talend.components.common.FixedConnectorsComponentProperties;
import org.talend.components.common.SchemaProperties;
import org.talend.daikon.properties.PropertiesList;
import org.talend.daikon.properties.presentation.Form;
import org.talend.daikon.properties.presentation.Widget;

public class AggregateProperties extends FixedConnectorsComponentProperties {

    public transient PropertyPathConnector MAIN_CONNECTOR = new PropertyPathConnector(Connector.MAIN_NAME, "main");

    public transient PropertyPathConnector FLOW_CONNECTOR =
            new PropertyPathConnector(Connector.MAIN_NAME, "schemaFlow");

    public SchemaProperties main = new SchemaProperties("main");

    public SchemaProperties schemaFlow = new SchemaProperties("schemaFlow");

    public PropertiesList<AggregateGroupProperties> groupBy =
            new PropertiesList<>("groupBy", new PropertiesList.NestedPropertiesFactory<AggregateGroupProperties>() {

                @Override
                public AggregateGroupProperties createAndInit(String name) {
                    return (AggregateGroupProperties) new AggregateGroupProperties(name).init();
                }
            });

    public PropertiesList<AggregateFunctionProperties> functions = new PropertiesList<>("functions",
            new PropertiesList.NestedPropertiesFactory<AggregateFunctionProperties>() {

                @Override
                public AggregateFunctionProperties createAndInit(String name) {
                    return (AggregateFunctionProperties) new AggregateFunctionProperties(name).init();
                }
            });

    public List<AggregateGroupProperties> filteredGroupBy() {
        List<AggregateGroupProperties> filteredGroupBy = new ArrayList<>();
        for (AggregateGroupProperties groupProps : groupBy.getPropertiesList()) {
            if (StringUtils.isEmpty(groupProps.columnName.getValue())) {
                continue;
            }
            filteredGroupBy.add(groupProps);
        }
        return filteredGroupBy;
    }

    public List<AggregateFunctionProperties> filteredFunctions() {
        List<AggregateFunctionProperties> filteredFunctions = new ArrayList<>();
        for (AggregateFunctionProperties funcProps : functions.getPropertiesList()) {
            if (StringUtils.isEmpty(funcProps.columnName.getValue())) {
                continue;
            }
            filteredFunctions.add(funcProps);
        }
        return filteredFunctions;
    }

    public AggregateProperties(String name) {
        super(name);
    }

    @Override
    public void setupLayout() {
        super.setupLayout();
        Form mainForm = new Form(this, Form.MAIN);
        mainForm.addRow(Widget.widget(groupBy).setWidgetType(Widget.NESTED_PROPERTIES).setConfigurationValue(
                Widget.NESTED_PROPERTIES_TYPE_OPTION, "filter"));
        mainForm.addRow(Widget.widget(functions).setWidgetType(Widget.NESTED_PROPERTIES).setConfigurationValue(
                Widget.NESTED_PROPERTIES_TYPE_OPTION, "filter"));
    }

    @Override
    public void setupProperties() {
        super.setupProperties();
        groupBy.init();
        groupBy.createAndAddRow();
        functions.init();
        functions.createAndAddRow();
    }

    @Override
    protected Set<PropertyPathConnector> getAllSchemaPropertiesConnectors(boolean isOutputConnection) {
        HashSet<PropertyPathConnector> connectors = new LinkedHashSet<PropertyPathConnector>();
        if (isOutputConnection) {
            connectors.add(FLOW_CONNECTOR);
        } else {
            connectors.add(MAIN_CONNECTOR);
        }
        return connectors;
    }
}
