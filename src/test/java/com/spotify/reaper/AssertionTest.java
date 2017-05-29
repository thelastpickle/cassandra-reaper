
package com.spotify.reaper;

import org.junit.Test;


public final class AssertionTest{

    @Test
    public void test_assertions_enabled(){
        boolean asserted = false;
        try{
            assert false;
        }catch (AssertionError error){
            asserted = true;
        }
        if (!asserted){
            throw new AssertionError("assertions are not enabled");
        }
    }

}