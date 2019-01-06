/*
 * chombo: Hadoop Map Reduce utility
 * Author: Pranab Ghosh
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 * Credit: Robert Sedgewick and Kevin Wayne of Princeton University 
 * Made additional  changes
 */

package org.chombo.math;

import java.io.Serializable;
import java.util.Objects;

public class Complex implements Serializable {
	private final double re;   
	private final double im;   
	
	/**
	 * create a new object with the given real and imaginary parts
	 * @param real
	 * @param imag
	 */
	public Complex(double real, double imag) {
		re = real;
		im = imag;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	public String toString() {
		if (im == 0) return re + "";
		if (re == 0) return im + "i";
		if (im <  0) return re + " - " + (-im) + "i";
	    return re + " + " + im + "i";
	}

	/**
	 * return abs/modulus/magnitude
	 * @return
	 */
	public double abs() {
		return Math.hypot(re, im);
	}

	/**
	 * return angle/phase/argument, normalized to be between -pi and pi
	 * @return
	 */
	public double phase() {
		return Math.atan2(im, re);
	}

	/**
	 * return a new Complex object whose value is (this + b)
	 * @param b
	 * @return
	 */
	public Complex plus(Complex b) {
		Complex a = this;             // invoking object
		double real = a.re + b.re;
		double imag = a.im + b.im;
		return new Complex(real, imag);
	}

	/**
	 * return a new Complex object whose value is (this - b)
	 * @param b
	 * @return
	 */
	public Complex minus(Complex b) {
		Complex a = this;
		double real = a.re - b.re;
		double imag = a.im - b.im;
		return new Complex(real, imag);
	}

	/**
	 * return a new Complex object whose value is (this * b)
	 * @param b
	 * @return
	 */
	public Complex times(Complex b) {
		Complex a = this;
		double real = a.re * b.re - a.im * b.im;
		double imag = a.re * b.im + a.im * b.re;
		return new Complex(real, imag);
	}

	/**
	 * return a new object whose value is (this * alpha)
	 * @param alpha
	 * @return
	 */
	public Complex scale(double alpha) {
		return new Complex(alpha * re, alpha * im);
	}

	/**
	 * return a new Complex object whose value is the conjugate of this
	 * @return
	 */
	public Complex conjugate() {
		return new Complex(re, -im);
	}

	/**
	 * return a new Complex object whose value is the reciprocal of this
	 * @return
	 */
	public Complex reciprocal() {
		double scale = re*re + im*im;
		return new Complex(re / scale, -im / scale);
	}

	/**
	 * real part
	 * @return
	 */
	public double re() {
		return re; 
	}
	
	/**
	 * imaginary part
	 * @return
	 */
	public double im() { 
		return im;
	}

	/**
	 * return a / b
	 * @param b
	 * @return
	 */
	public Complex divides(Complex b) {
		Complex a = this;
		return a.times(b.reciprocal());
	}

	/**
	 * return a new Complex object whose value is the complex exponential of this
	 * @return
	 */
	public Complex exp() {
		return new Complex(Math.exp(re) * Math.cos(im), Math.exp(re) * Math.sin(im));
	}

	/**
	 * return a new Complex object whose value is the complex sine of this
	 * @return
	 */
	public Complex sin() {
		return new Complex(Math.sin(re) * Math.cosh(im), Math.cos(re) * Math.sinh(im));
	}

	/**
	 * return a new Complex object whose value is the complex cosine of this
	 * @return
	 */
	public Complex cos() {
		return new Complex(Math.cos(re) * Math.cosh(im), -Math.sin(re) * Math.sinh(im));
	}

	/**
	 * return a new Complex object whose value is the complex tangent of this
	 * @return
	 */
	public Complex tan() {
		return sin().divides(cos());
	}

	/**
	 * a static version of plus
	 * @param a
	 * @param b
	 * @return
	 */
	public static Complex plus(Complex a, Complex b) {
		double real = a.re + b.re;
		double imag = a.im + b.im;
		Complex sum = new Complex(real, imag);
		return sum;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	public boolean equals(Object x) {
		if (x == null) return false;
		if (this.getClass() != x.getClass()) return false;
		Complex that = (Complex) x;
		return (this.re == that.re) && (this.im == that.im);
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	public int hashCode() {
		return Objects.hash(re, im);
	}
}
