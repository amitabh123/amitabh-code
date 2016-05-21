package complex;

import java.util.List;
import java.util.Vector;


public class CN {

    private static List<CN> stack = new Vector<CN>();

    private final static int special_none = -1;
    private final static int special_zero = special_none + 1;
    private final static int special_one = special_zero + 1;
    private final static int special_i = special_one + 1;
    private final static int special_pi = special_i + 1;
    private final static int special_et = special_pi + 1;

    public static final CN ZERO = new CN(special_zero);
    public static final CN ONE = new CN(special_one);

    public static CN I = new CN(special_i);
    public static CN PI = new CN(special_pi);
    public static CN ET = new CN(special_et);

    private BD realPart;    
    private BD imagPart;    
    private BD modulus;     
    private BD argument;     

    private int special;

    CN(int s) {
        setSpecial(s);
    }

    public static CN valueOf(String string, String stackString) {
        if (string.startsWith(stackString)) {
            return CN.get(Integer.valueOf(string.substring(stackString.length())));
        }
        // parse string. If it is of the type <number> or <number>i, return value else throw exception

        BD realValue = BD.ZERO, imagValue = BD.ZERO, modValue = BD.ZERO, argValue = BD.ZERO;
        int l = string.length();
        if (l > 0) {
            if (string.charAt(0) == '-') {  // minus is taken care of in parser. We dont want it here
                return null;
            }
            if (string.charAt(l - 1) == 'i') {
                argValue = BD.PIdiv2;
                modValue = new BD(string.substring(0, l - 1)); 
                imagValue = modValue;
            } else {
                modValue = new BD(string); 
                realValue = modValue;
            }
        }
        return new CN(realValue, imagValue, modValue, argValue);
    }

    private CN() {
        setSpecial(special_none);
    }

    public CN(BD realValue, BD imagValue, BD modValue, BD argValue) {
        this();
        this.realPart = realValue;
        this.imagPart = imagValue;
        this.modulus = modValue;
        this.argument = argValue;
    }

    private void setSpecial(int s) {
        this.special = s;
        switch (s) {
            case special_zero:
                this.realPart = BD.ZERO;
                this.imagPart = BD.ZERO;
                this.modulus = BD.ZERO;
                this.argument = BD.ZERO;
                break;
            case special_one:
                this.realPart = BD.ONE;
                this.imagPart = BD.ZERO;
                this.modulus = BD.ONE;
                this.argument = BD.ZERO;
                break;
            case special_i:
                this.realPart = BD.ZERO;
                this.imagPart = BD.ONE;
                this.modulus = BD.ONE;
                this.argument = BD.PIdiv2;
                break;
            case special_pi:
                this.realPart = BD.PI;
                this.imagPart = BD.ZERO;
                this.modulus = BD.PI;
                this.argument = BD.ZERO;
                break;
            case special_et:
                this.realPart = BD.ET;
                this.imagPart = BD.ZERO;
                this.modulus = BD.ET;
                this.argument = BD.ZERO;
                break;
            case special_none:
                this.realPart = null;
                this.imagPart = null;
                this.modulus = null;
                this.argument = null;
                break;
            default:
                throw new UnsupportedOperationException("Not yet implemented");
        }        
    }

    public String format(boolean eform) {
        if (eform) {
            return polarString();
        }
        return rectangularString();
    }

    String polarString() {
        if (modulus == null || argument == null) {
            return ("0 @ 0");
        }
        return modulus.format() + " @ " + argument.format();
    }

    String rectangularString() {
        if (realPart == null || imagPart == null) {
            return ("0");
        }
        BD smallValue = BD.ZERO; 
        BD smallValue2 = BD.ZERO; 
        
        BD unsignedImag = imagPart.abs();
        String s1 = "";
        if (realPart.abs().compareTo(smallValue) > 0) {
            
            s1 += (realPart.format());
        } else {
            if (unsignedImag.compareTo(smallValue2) <= 0) {
                s1 += "0";
            }
        }
        String tmp = (unsignedImag.format());
        if (!tmp.equals("0")) { 
            
            s1 += ((imagPart.compareTo(BD.ZERO) > 0) ? " + " : " - ");
            s1 += (tmp + " i");
        }
        if (s1.isEmpty()) return ("0");
        
        return s1;
    }

    public String addToStack(String stackString) {
        stack.add(this);
        return stackString + String.valueOf(stack.size());
    }

    public static void stackReset() {
        stack.clear();
    }

    public CN negate() {
        CN temp = new CN();
        temp.realPart = this.realPart.negate();
        temp.imagPart = this.imagPart.negate();
        temp.modulus = this.modulus;
        temp.argument = this.argument.add(BD.PI);
        return temp;
    }

    private static CN get(int i) {       
        return stack.get(i - 1);
    }

    public CN subtract(CN right) {
        CN temp = new CN();
        temp.realPart = this.realPart.subtract(right.realPart);
        temp.imagPart = this.imagPart.subtract(right.imagPart);
        temp.setPolar();
        return temp;
    }

    public CN add(CN right) {
        CN temp = new CN();
        temp.realPart = this.realPart.add(right.realPart);
        temp.imagPart = this.imagPart.add(right.imagPart);
        temp.setPolar();
        return temp;
    }

    public CN divide(CN right) {
        CN temp = new CN();
        BD x = ((right.realPart.sqr()).add(right.imagPart.sqr()));
        temp.realPart = ((this.realPart.multiply(right.realPart)).add(this.imagPart.multiply(right.imagPart))).divide(x);
        temp.imagPart = ((this.imagPart.multiply(right.realPart)).subtract(this.realPart.multiply(right.imagPart))).divide(x);
        temp.setPolar();
        return temp;
    }

    public CN multiply(CN right) {
        CN temp = new CN();
        temp.realPart = (this.realPart.multiply(right.realPart)).subtract(this.imagPart.multiply(right.imagPart));
        temp.imagPart = (this.realPart.multiply(right.imagPart)).add(this.imagPart.multiply(right.realPart));
        temp.setPolar();
        return temp;
    }

    public CN pow(CN right) {
        CN temp = new CN();
        if (this.modulus.equals(BD.ZERO)) {
            if (right.modulus.equals(BD.ZERO)){
                throw new ArithmeticException("illegal operation");
            }
            return CN.ZERO;
        }
        temp.modulus = (this.modulus.pow(right.realPart)).multiply(
                ((right.imagPart.negate()).multiply(this.argument)).exp());
        temp.argument = (this.argument.multiply(right.realPart)).add((this.modulus.log()).multiply(right.imagPart));
        temp.setRectangular();
        return temp;
    }

    public CN sqrt() {
        CN temp = new CN();
        temp.modulus = this.modulus.sqrt();
        temp.argument = this.argument.div2();
        temp.setRectangular();
        return temp;
    }

    public CN ln() {
        CN temp = new CN();
        temp.realPart = this.modulus.log();
        temp.imagPart = this.argument;
        temp.setPolar();
        return temp;
    }

    public CN exp() {
        CN temp = new CN();
        temp.modulus = this.realPart.exp();
        temp.argument = this.imagPart;
        temp.setRectangular();
        return temp;
    }

    public CN euler(CN right) {
        CN temp = new CN();
        temp.modulus = this.realPart;
        temp.argument = right.realPart;
        temp.setRectangular();
        return temp;
    }

    public CN sin() {
        BD c = this.realPart;
        BD d = (this.imagPart).exp();
        CN temp = new CN();
        temp.realPart = ((c.sin()).multiply(d.add(BD.ONE.divide(d)))).div2();
        temp.imagPart = (c.cos()).multiply(d.subtract(BD.ONE.divide(d))).div2();
        temp.setPolar();
        return temp;
    }

    public CN cos() {
        BD c = this.realPart;
        BD d = this.imagPart.exp();
        CN temp = new CN();
        temp.realPart = (c.cos()).multiply((d.add(BD.ONE.divide(d))).div2());
        temp.imagPart = ((c.sin()).negate()).multiply((d.subtract(BD.ONE.divide(d))).div2());
        temp.setPolar();
        return temp;
    }

    public CN tan() {
        return this.sin().divide(this.cos());
    }

    public CN asin() {
        BD x = (this.modulus.sqr().multiply((this.argument.mul2()).cos())).subtract(BD.ONE);
        BD y = (this.modulus.sqr().multiply((this.argument.mul2()).sin()));
        CN c = new CN();
        c.modulus = (((x.sqr()).add(y.sqr())).sqrt()).sqrt();
        c.argument = (BD.atan2(x, y)).div2();
        c.setRectangular();
        x = (this.realPart).add(c.realPart);
        y = (this.imagPart).add(c.imagPart);
        CN temp = new CN();
        temp.imagPart = ((((x.sqr()).add(y.sqr())).sqrt()).log()).negate();
        temp.realPart = (BD.atan2(x, y)).add(BD.PIdiv2);
        temp.setPolar();
        return temp;
    }

    public CN acos() {
        BD x = (this.modulus.sqr().multiply((this.argument.mul2()).cos())).subtract(BD.ONE);
        BD y = (this.modulus.sqr().multiply((this.argument.mul2()).sin()));
        CN c = new CN();
        c.modulus = (((x.sqr()).add(y.sqr())).sqrt()).sqrt();
        c.argument = (BD.atan2(x, y)).div2();
        c.setRectangular();
        x = (this.realPart).add(c.realPart);
        y = (this.imagPart).add(c.imagPart);
        CN temp = new CN();
        temp.imagPart = ((((x.sqr()).add(y.sqr())).sqrt()).log()).negate();
        temp.realPart = (BD.atan2(x, y));
        temp.setPolar();
        return temp;
    }

    public CN atan() {
        BD c = this.imagPart;
        BD d = this.realPart;
        CN temp = new CN();
        temp.realPart = ((BD.atan2(BD.ONE.subtract(c), d)).subtract(BD.atan2(BD.ONE.add(c), d.negate()))).div2();
        temp.imagPart = ((BD.ONE.subtract(c)).sqr()).add(d.sqr());
        if (temp.imagPart.equalZero()) {
            throw new ArithmeticException("DIV: attempted division by zero");
        } else {
            temp.imagPart = ((((c.add(BD.ONE)).sqr()).add(d.sqr())).divide(temp.imagPart)).log().divide(new BD("4"));
        }
        temp.setPolar();
        return temp;
    }

    public CN sinh() {
        return ((this.multiplyI()).sin()).divideI();
    }

    public CN tanh() {
        return ((this.multiplyI()).tan()).divideI();
    }

    public CN cosh() {
        return ((this.multiplyI()).cos());
    }

    public CN asinh() {
        return (this.add(((this.multiply(this)).add(ONE)).sqrt())).ln();
    }

    public CN atanh() {
        return ((this.multiplyI()).atan()).divideI();
    }

    public CN acosh() {
        return (this.add(((this.multiply(this)).subtract(ONE)).sqrt())).ln();        
    }

    public CN real() {
        CN temp = new CN();
        temp.realPart = this.realPart;
        temp.imagPart = BD.ZERO;
        temp.argument = BD.ZERO.add(this.realPart.compareTo(BD.ZERO) < 0 ? BD.PI : BD.ZERO);
        temp.modulus = this.realPart.abs();
        return temp;
    }

    public CN imag() {
        CN temp = new CN();
        temp.realPart = BD.ZERO;
        temp.argument = BD.PIdiv2.subtract(this.imagPart.compareTo(BD.ZERO) < 0 ? BD.PI : BD.ZERO);
        temp.modulus = this.imagPart.abs();
        temp.imagPart = this.imagPart;
        return temp;
    }

    public CN conj() {
        CN temp = new CN();
        temp.imagPart = this.imagPart.negate();
        temp.argument = this.argument.negate();
        temp.realPart = this.realPart;
        temp.modulus = this.modulus;
        return temp;
    }

    public CN arg() {
        CN temp = new CN();
        temp.realPart = this.argument;
        temp.imagPart = BD.ZERO;
        temp.modulus = this.argument.abs();
        temp.argument = (this.argument.compareTo(BD.ZERO) < 0) ? BD.PI : BD.ZERO;
        return temp;        
    }

    public CN mod() {
        CN temp = new CN(); 
        temp.realPart = this.modulus;
        temp.imagPart = BD.ZERO;
        temp.modulus = this.modulus;
        temp.argument = BD.ZERO;
        return temp;
    }

    private void setPolar() {
        this.modulus = ((this.realPart.sqr()).add(this.imagPart.sqr())).sqrt();        
        this.argument = BD.atan2(realPart, imagPart);        
        this.special = special_none;
    }

    private void setRectangular() {
        this.realPart = ((this.argument.cos()).multiply(this.modulus));
        this.imagPart = ((this.argument.sin()).multiply(this.modulus));
        this.special = special_none;
    }

    private CN multiplyI() {
        CN temp = new CN();
        temp.argument = this.argument.add(BD.PIdiv2);
        temp.imagPart = this.realPart;
        temp.realPart = this.imagPart.negate();
        temp.modulus = this.modulus;
        return temp;
    }

    private CN divideI() {
        return this.multiplyI().negate();  
    }
}
