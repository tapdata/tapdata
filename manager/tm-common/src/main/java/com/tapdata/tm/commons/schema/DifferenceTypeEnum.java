package com.tapdata.tm.commons.schema;

import java.util.List;

public enum DifferenceTypeEnum {
    Additional{
        @Override
        public void processDifferenceField(Field field, List<Field> fields, DifferenceField differenceField) {
            if(!fields.contains(differenceField.getTargetField())) {
                fields.add(differenceField.getTargetField());
            }
        }
        @Override
        public void recoverField(Field field, List<Field> fields, DifferenceField differenceField) {
            fields.remove(field);
        }
    },
    Missing{
        @Override
        public void processDifferenceField(Field field, List<Field> fields, DifferenceField differenceField) {
            fields.remove(field);
        }
        @Override
        public void recoverField(Field field, List<Field> fields, DifferenceField differenceField) {
            if(!fields.contains(differenceField.getSourceField())) {
                fields.add(differenceField.getSourceField());
            }

        }
    },
    Different{
        @Override
        public void processDifferenceField(Field field, List<Field> fields, DifferenceField differenceField) {
            if(field.getDataType().equals(differenceField.getSourceField().getDataType())) {
                differenceField.getTargetField().setOriginalFieldName(field.getOriginalFieldName());
                differenceField.getTargetField().setFieldName(field.getFieldName());
                fields.remove(field);
                fields.add(differenceField.getTargetField());
            }
        }
        @Override
        public void recoverField(Field field, List<Field> fields, DifferenceField differenceField) {
            if(field.getDataType().equals(differenceField.getTargetField().getDataType())) {
                fields.remove(field);
                fields.add(differenceField.getSourceField());
            }
        }
    },
    Precision{
        @Override
        public void processDifferenceField(Field field, List<Field> fields, DifferenceField differenceField) {
            if(field.getDataType().equals(differenceField.getSourceField().getDataType())) {
                differenceField.getTargetField().setOriginalFieldName(field.getOriginalFieldName());
                differenceField.getTargetField().setFieldName(field.getFieldName());
                fields.remove(field);
                fields.add(differenceField.getTargetField());
            }
        }
        @Override
        public void recoverField(Field field, List<Field> fields, DifferenceField differenceField) {
            if(field.getDataType().equals(differenceField.getTargetField().getDataType())) {
                fields.remove(field);
                fields.add(differenceField.getSourceField());
            }
        }
    },
    CannotWrite{
        @Override
        public void processDifferenceField(Field field, List<Field> fields, DifferenceField differenceField) {
            fields.remove(field);
        }
        @Override
        public void recoverField(Field field, List<Field> fields, DifferenceField differenceField) {
            if(!fields.contains(differenceField.getSourceField())) {
                fields.add(differenceField.getSourceField());
            }
        }
    },
    PrimaryKeyInconsistency{
        @Override
        public void processDifferenceField(Field field, List<Field> fields, DifferenceField differenceField) {

        }
        @Override
        public void recoverField(Field field, List<Field> fields, DifferenceField differenceField) {

        }
    };

    public abstract void processDifferenceField(Field field, List<Field> fields, DifferenceField differenceField);

    public abstract void recoverField(Field field, List<Field> fields, DifferenceField differenceField);

}
