package org.apache.jmeter.protocol.mqtt.control.gui;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import javax.swing.AbstractButton;
import javax.swing.ButtonGroup;
import javax.swing.ButtonModel;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;
//import org.apache.jmeter.util.JMeterUtils;
import org.apache.jorphan.gui.JLabeledField;

public class JLabeledRadioI18N extends JPanel implements JLabeledField, ActionListener {
    private static final long serialVersionUID = 240L;
    private final JLabel mLabel = new JLabel();
    private final ButtonGroup bGroup = new ButtonGroup();
    private final ArrayList<ChangeListener> mChangeListeners = new ArrayList(3);

    public JLabeledRadioI18N(String label_resouce, String[] item_resources, String selectedItem) {
        this.setLabel(label_resouce);
        this.init(item_resources, selectedItem);
    }

    /** @deprecated */
    @Deprecated
    public JLabeledRadioI18N() {
    }

    private void init(String[] resouces, String selected) {
        this.add(this.mLabel);
        this.initButtonGroup(resouces, selected);
    }

    private void initButtonGroup(String[] resouces, String selected) {
        for(int idx = 0; idx < resouces.length; ++idx) {
            JRadioButton btn = new JRadioButton(resouces[idx]);
            btn.setActionCommand(resouces[idx]);
            btn.addActionListener(this);
            this.bGroup.add(btn);
            this.add(btn);
            if (selected != null && selected.equals(resouces[idx])) {
                btn.setSelected(true);
            }
        }

    }

    public void resetButtons(String[] resouces, String selected) {
        Enumeration<AbstractButton> buttons = this.bGroup.getElements();
        ArrayList buttonsToRemove = new ArrayList(this.bGroup.getButtonCount());

        while(buttons.hasMoreElements()) {
            AbstractButton abstractButton = (AbstractButton)buttons.nextElement();
            buttonsToRemove.add(abstractButton);
        }

        Iterator i$ = buttonsToRemove.iterator();

        AbstractButton abstractButton;
        while(i$.hasNext()) {
            abstractButton = (AbstractButton)i$.next();
            abstractButton.removeActionListener(this);
            this.bGroup.remove(abstractButton);
        }

        i$ = buttonsToRemove.iterator();

        while(i$.hasNext()) {
            abstractButton = (AbstractButton)i$.next();
            this.remove(abstractButton);
        }

        this.initButtonGroup(resouces, selected);
    }

    public String getText() {
        return this.bGroup.getSelection().getActionCommand();
    }

    public void setText(String resourcename) {
        Enumeration en = this.bGroup.getElements();

        while(en.hasMoreElements()) {
            ButtonModel model = ((AbstractButton)en.nextElement()).getModel();
            if (model.getActionCommand().equals(resourcename)) {
                this.bGroup.setSelected(model, true);
            } else {
                this.bGroup.setSelected(model, false);
            }
        }

    }

    public final void setLabel(String label_resource) {
        this.mLabel.setText(label_resource);
    }

    public void addChangeListener(ChangeListener pChangeListener) {
        this.mChangeListeners.add(pChangeListener);
    }

    private void notifyChangeListeners() {
        ChangeEvent ce = new ChangeEvent(this);

        for(int index = 0; index < this.mChangeListeners.size(); ++index) {
            ((ChangeListener)this.mChangeListeners.get(index)).stateChanged(ce);
        }

    }

    public List<JComponent> getComponentList() {
        List<JComponent> comps = new LinkedList();
        comps.add(this.mLabel);
        Enumeration en = this.bGroup.getElements();

        while(en.hasMoreElements()) {
            comps.add((JComponent) en.nextElement());
        }

        return comps;
    }

    public void actionPerformed(ActionEvent e) {
        this.notifyChangeListeners();
    }
}
