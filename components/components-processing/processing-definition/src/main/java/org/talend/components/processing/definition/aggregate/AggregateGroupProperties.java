package org.talend.components.processing.definition.aggregate;

import org.talend.daikon.properties.PropertiesImpl;
import org.talend.daikon.properties.presentation.Form;
import org.talend.daikon.properties.presentation.Widget;
import org.talend.daikon.properties.property.Property;
import org.talend.daikon.properties.property.PropertyFactory;

public class AggregateGroupProperties extends PropertiesImpl {

    public AggregateGroupProperties(String name) {
        super(name);
    }

    /**
     * This enum will be filled with the name of the input columns.
     */
    public Property<String> columnName = PropertyFactory.newString("columnName", "").setRequired();

    @Override
    public void setupLayout() {
        super.setupLayout();
        Form mainForm = new Form(this, Form.MAIN);
        mainForm.addRow(Widget.widget(columnName).setWidgetType(Widget.DATALIST_WIDGET_TYPE));
    }
}
