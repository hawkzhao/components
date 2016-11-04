// ============================================================================
//
// Copyright (C) 2006-2015 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components.api.service.common;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.talend.components.api.ComponentFamilyDefinition;
import org.talend.components.api.ComponentInstaller;
import org.talend.components.api.RuntimableDefinition;
import org.talend.components.api.component.ComponentDefinition;
import org.talend.components.api.wizard.ComponentWizardDefinition;
import org.talend.daikon.definition.Definition;
import org.talend.daikon.definition.service.DefinitionRegistryService;
import org.talend.daikon.exception.TalendRuntimeException;

/**
 * Utility for getting and setting the definitions registered in the component framework.
 *
 * This should be populated by all of the {@link ComponentInstaller} instances found, using the methods from
 * {@link ComponentInstaller.ComponentFrameworkContext}. Once it has been been initialized, it can be used by the
 * {@link org.talend.components.api.service.ComponentService} implementations.
 */
public class DefinitionRegistry implements ComponentInstaller.ComponentFrameworkContext, DefinitionRegistryService {

    /**
     * All of the {@link RuntimableDefinition}s that have been added to the framework, including
     * {@link ComponentDefinition}s.
     */
    private Map<String, Definition> definitions;

    private Map<String, ComponentFamilyDefinition> componentFamilies;

    public DefinitionRegistry() {
        reset();
    }

    /**
     * @return a list of all the extended {@link RuntimableDefinition} that have been added to the framework.
     */
    public Iterable<Definition> getIterableDefinitions() {
        return definitions.values();
    }

    /**
     * @return a map of all the extended {@link RuntimableDefinition} that have been added to the framework keyed with their
     *         unique name.
     */
    public Map<String, Definition> getDefinitions() {
        return definitions;
    }

    /**
     * @return a subset of the known definitions
     */
    public <T extends Definition> Iterable<T> getDefinitionsByType(final Class<T> cls) {
        // If we ever add a guava dependency: return Iterables.filter(definitions, cls);
        List<T> byType = new ArrayList<>();
        for (Definition def : getIterableDefinitions()) {
            if (cls.isAssignableFrom(def.getClass())) {
                byType.add((T) def);
            }
        }
        return byType;
    }

    @Override
    public <T extends Definition> Map<String, T> getDefinitionsMapByType(Class<T> cls) {
        Map<String, T> definitionsAsMap = new HashMap<>();
        for (T def : getDefinitionsByType(cls)) {
            definitionsAsMap.put(def.getName(), def);
        }
        return definitionsAsMap;
    }

    /**
     * @return a map of component families using their name as a key, never null.
     */
    public Map<String, ComponentFamilyDefinition> getComponentFamilies() {
        return componentFamilies;
    }

    /**
     * Remove all known definitions that are stored in the registry and returns it to a modifiable, empty state.
     */
    public void reset() {
        definitions = new HashMap<>();
        componentFamilies = new HashMap<>();
    }

    /**
     * After all of the definitions have been stored in the registry, ensure that no further modifications can be made.
     */
    public void lock() {
        definitions = Collections.unmodifiableMap(definitions);
        componentFamilies = Collections.unmodifiableMap(componentFamilies);
    }

    @Override
    public void registerDefinition(Iterable<? extends Definition> defs) {
        for (Definition def : defs) {
            Definition previousValue = definitions.put(def.getName(), def);
            if (previousValue != null) {// we cannot have 2 definiions with the same name
                throw TalendRuntimeException.createUnexpectedException(
                        "2 definitions have the same name [" + previousValue.getName() + "] but their name must be unique.");
            }
        }
    }

    @Override
    public void registerComponentWizardDefinition(Iterable<? extends ComponentWizardDefinition> defs) {
        registerDefinition(defs);
    }

    @Override
    public void registerComponentFamilyDefinition(ComponentFamilyDefinition componentFamily) {
        getComponentFamilies().put(componentFamily.getName(), componentFamily);
        // Always automatically register the nested definitions in the component family.
        registerDefinition(componentFamily.getComponentWizards());
        registerDefinition(componentFamily.getDefinitions());
    }

    @Override
    public InputStream getImage(String definitionName) {
        Definition def = getDefinitions().get(definitionName);
        if (def == null) {
            throw TalendRuntimeException
                    .createUnexpectedException("fails to retrieve any definition for the name [" + definitionName + "].");
        }
        return ComponentServiceImpl.getImageStream(def, def.getImagePath());
    }
}