package org.talend.components.processing.definition.aggregate;

import org.talend.daikon.properties.PropertiesImpl;
import org.talend.daikon.properties.presentation.Form;
import org.talend.daikon.properties.presentation.Widget;
import org.talend.daikon.properties.property.EnumProperty;
import org.talend.daikon.properties.property.Property;
import org.talend.daikon.properties.property.PropertyFactory;

public class AggregateFunctionProperties extends PropertiesImpl {

    public AggregateFunctionProperties(String name) {
        super(name);
    }

    /**
     * This enum will be filled with the name of the input columns.
     */
    public Property<String> columnName = PropertyFactory.newString("columnName", "").setRequired();

    /**
     * This enum represent the function applicable to the input value when grouping. The functions
     * displayed by the UI are dependent of the type of the columnName.
     *
     * If columnName's type is numerical (Integer, Long, Float or Double), Function will contain "SUM, AVG, MIN, MAX"
     * and "COUNT, LIST"
     *
     * If columnName's type is not numerical, Function will contain "COUNT, LIST" only
     *
     */
    public EnumProperty<GroupingFunction> groupingFunction =
            PropertyFactory.newEnum("groupingFunction", GroupingFunction.class);

    @Override
    public void setupLayout() {
        super.setupLayout();
        Form mainForm = new Form(this, Form.MAIN);
        mainForm.addRow(Widget.widget(columnName).setWidgetType(Widget.DATALIST_WIDGET_TYPE));
        mainForm.addRow(groupingFunction);
    }

    @Override
    public void setupProperties() {
        super.setupProperties();
        groupingFunction.setValue(GroupingFunction.LIST);
    }

    public enum AggregateFunctionType {

    }
}
