/* -------------------------------------------------
 * Projekt:     hydra-offer-dumps
 * Datum:       $Date: 2009-02-26 12:04:18 +0100 (Do, 26 Feb 2009) $
 * Autor:       $Author: michael.krkoska $
 * Version:     $Revision: 1471 $
 * Datei:       $Source$
 * angelegt:    20.07.2007 10:02:13
 * --------------------------------------------------
 * Copyright (c) Mentasys GmbH, 2007
 * --------------------------------------------------
 */
package util.dump.externalization;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Random;


public class DumpOfferBeanExternalizable implements Externalizable, Comparable<DumpOfferBeanExternalizable> {

   private static final long   RANDOM_SEED      = 123456;
   private static final Random RANDOM           = new Random(RANDOM_SEED);

   private static final long   serialVersionUID = 432200534464637912L;


   public static DumpOfferBeanExternalizable createRandomBean() {
      DumpOfferBeanExternalizable d = new DumpOfferBeanExternalizable();
      d.code = randomString();
      d.codeType = RANDOM.nextInt();
      d.contentId = RANDOM.nextLong();
      d.created = RANDOM.nextInt();
      d.deeplink = randomString();
      d.deliveryCost = randomString();
      d.deliveryCostValue = RANDOM.nextDouble();
      d.deliveryTime = randomString();
      d.deliveryTimeColor = RANDOM.nextInt();
      d.description = randomString();
      d.hid = RANDOM.nextLong();
      d.imageSmall = randomString();
      d.imageView = randomString();
      d.isActive = RANDOM.nextInt() % 2 == 0 ? true : false;
      d.manufacturer = randomString();
      d.manufacturerPrdId = randomString();
      d.matchOwner = RANDOM.nextLong();
      d.matchProgram = RANDOM.nextLong();
      d.matchStamp = RANDOM.nextLong();
      d.matchType = RANDOM.nextLong();
      d.name = randomString();
      d.oldPrice = RANDOM.nextDouble();
      d.ownerId = RANDOM.nextLong();
      d.parentHid = RANDOM.nextLong();
      d.price = RANDOM.nextDouble();
      d.promoText = randomString();
      d.sourceGroup = randomString();
      d.sourceId = randomString();
      d.updated = RANDOM.nextLong();
      d.variant = RANDOM.nextLong();
      return d;
   }

   private static String randomString() {
      int length = RANDOM.nextInt(50);
      StringBuilder sb = new StringBuilder(length);
      for ( int i = 0; i < length; i++ ) {
         sb.append((char)(RANDOM.nextInt(64) + 32));
      }
      return sb.toString();
   }

   public long    contentId;
   public long    ownerId;
   public String  sourceId;
   public String  sourceGroup;

   public boolean isActive;

   public long    created;
   public long    updated;

   public long    hid;
   public long    parentHid;
   public long    variant;

   public long    matchOwner;
   public long    matchProgram;
   public long    matchType;
   public long    matchStamp;

   public String  name;
   public String  description;
   public String  manufacturer;
   public String  manufacturerPrdId;
   public String  promoText;

   public double  price;
   public Double  oldPrice;

   public String  code;
   public int     codeType;

   public String  deeplink;

   public String  deliveryTime;
   public int     deliveryTimeColor;
   public String  deliveryCost;
   public Double  deliveryCostValue;

   public String  imageView;
   public String  imageSmall;


   public DumpOfferBeanExternalizable() {}

   public int compareTo( DumpOfferBeanExternalizable o ) {
      return this.contentId < o.contentId ? -1 : (this.contentId == o.contentId ? 0 : 1);
   }

   @Override
   public boolean equals( Object obj ) {
      if ( this == obj ) {
         return true;
      }
      if ( obj == null ) {
         return false;
      }
      if ( getClass() != obj.getClass() ) {
         return false;
      }
      DumpOfferBeanExternalizable other = (DumpOfferBeanExternalizable)obj;
      if ( code == null ) {
         if ( other.code != null ) {
            return false;
         }
      }
      else if ( !code.equals(other.code) ) {
         return false;
      }
      if ( codeType != other.codeType ) {
         return false;
      }
      if ( contentId != other.contentId ) {
         return false;
      }
      if ( created != other.created ) {
         return false;
      }
      if ( deeplink == null ) {
         if ( other.deeplink != null ) {
            return false;
         }
      }
      else if ( !deeplink.equals(other.deeplink) ) {
         return false;
      }
      if ( deliveryCost == null ) {
         if ( other.deliveryCost != null ) {
            return false;
         }
      }
      else if ( !deliveryCost.equals(other.deliveryCost) ) {
         return false;
      }
      if ( deliveryCostValue == null ) {
         if ( other.deliveryCostValue != null ) {
            return false;
         }
      }
      else if ( !deliveryCostValue.equals(other.deliveryCostValue) ) {
         return false;
      }
      if ( deliveryTime == null ) {
         if ( other.deliveryTime != null ) {
            return false;
         }
      }
      else if ( !deliveryTime.equals(other.deliveryTime) ) {
         return false;
      }
      if ( deliveryTimeColor != other.deliveryTimeColor ) {
         return false;
      }
      if ( description == null ) {
         if ( other.description != null ) {
            return false;
         }
      }
      else if ( !description.equals(other.description) ) {
         return false;
      }
      if ( hid != other.hid ) {
         return false;
      }
      if ( imageSmall == null ) {
         if ( other.imageSmall != null ) {
            return false;
         }
      }
      else if ( !imageSmall.equals(other.imageSmall) ) {
         return false;
      }
      if ( imageView == null ) {
         if ( other.imageView != null ) {
            return false;
         }
      }
      else if ( !imageView.equals(other.imageView) ) {
         return false;
      }
      if ( isActive != other.isActive ) {
         return false;
      }
      if ( manufacturer == null ) {
         if ( other.manufacturer != null ) {
            return false;
         }
      }
      else if ( !manufacturer.equals(other.manufacturer) ) {
         return false;
      }
      if ( manufacturerPrdId == null ) {
         if ( other.manufacturerPrdId != null ) {
            return false;
         }
      }
      else if ( !manufacturerPrdId.equals(other.manufacturerPrdId) ) {
         return false;
      }
      if ( matchOwner != other.matchOwner ) {
         return false;
      }
      if ( matchProgram != other.matchProgram ) {
         return false;
      }
      if ( matchStamp != other.matchStamp ) {
         return false;
      }
      if ( matchType != other.matchType ) {
         return false;
      }
      if ( name == null ) {
         if ( other.name != null ) {
            return false;
         }
      }
      else if ( !name.equals(other.name) ) {
         return false;
      }
      if ( oldPrice == null ) {
         if ( other.oldPrice != null ) {
            return false;
         }
      }
      else if ( !oldPrice.equals(other.oldPrice) ) {
         return false;
      }
      if ( ownerId != other.ownerId ) {
         return false;
      }
      if ( parentHid != other.parentHid ) {
         return false;
      }
      if ( Double.doubleToLongBits(price) != Double.doubleToLongBits(other.price) ) {
         return false;
      }
      if ( promoText == null ) {
         if ( other.promoText != null ) {
            return false;
         }
      }
      else if ( !promoText.equals(other.promoText) ) {
         return false;
      }
      if ( sourceGroup == null ) {
         if ( other.sourceGroup != null ) {
            return false;
         }
      }
      else if ( !sourceGroup.equals(other.sourceGroup) ) {
         return false;
      }
      if ( sourceId == null ) {
         if ( other.sourceId != null ) {
            return false;
         }
      }
      else if ( !sourceId.equals(other.sourceId) ) {
         return false;
      }
      if ( updated != other.updated ) {
         return false;
      }
      if ( variant != other.variant ) {
         return false;
      }
      return true;
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((code == null) ? 0 : code.hashCode());
      result = prime * result + codeType;
      result = prime * result + (int)(contentId ^ (contentId >>> 32));
      result = prime * result + (int)(created ^ (created >>> 32));
      result = prime * result + ((deeplink == null) ? 0 : deeplink.hashCode());
      result = prime * result + ((deliveryCost == null) ? 0 : deliveryCost.hashCode());
      result = prime * result + ((deliveryCostValue == null) ? 0 : deliveryCostValue.hashCode());
      result = prime * result + ((deliveryTime == null) ? 0 : deliveryTime.hashCode());
      result = prime * result + deliveryTimeColor;
      result = prime * result + ((description == null) ? 0 : description.hashCode());
      result = prime * result + (int)(hid ^ (hid >>> 32));
      result = prime * result + ((imageSmall == null) ? 0 : imageSmall.hashCode());
      result = prime * result + ((imageView == null) ? 0 : imageView.hashCode());
      result = prime * result + (isActive ? 1231 : 1237);
      result = prime * result + ((manufacturer == null) ? 0 : manufacturer.hashCode());
      result = prime * result + ((manufacturerPrdId == null) ? 0 : manufacturerPrdId.hashCode());
      result = prime * result + (int)(matchOwner ^ (matchOwner >>> 32));
      result = prime * result + (int)(matchProgram ^ (matchProgram >>> 32));
      result = prime * result + (int)(matchStamp ^ (matchStamp >>> 32));
      result = prime * result + (int)(matchType ^ (matchType >>> 32));
      result = prime * result + ((name == null) ? 0 : name.hashCode());
      result = prime * result + ((oldPrice == null) ? 0 : oldPrice.hashCode());
      result = prime * result + (int)(ownerId ^ (ownerId >>> 32));
      result = prime * result + (int)(parentHid ^ (parentHid >>> 32));
      long temp;
      temp = Double.doubleToLongBits(price);
      result = prime * result + (int)(temp ^ (temp >>> 32));
      result = prime * result + ((promoText == null) ? 0 : promoText.hashCode());
      result = prime * result + ((sourceGroup == null) ? 0 : sourceGroup.hashCode());
      result = prime * result + ((sourceId == null) ? 0 : sourceId.hashCode());
      result = prime * result + (int)(updated ^ (updated >>> 32));
      result = prime * result + (int)(variant ^ (variant >>> 32));
      return result;
   }

   public void readExternal( ObjectInput in ) throws IOException, ClassNotFoundException {
      contentId = in.readLong();
      ownerId = in.readLong();
      sourceId = in.readBoolean() ? in.readUTF() : null;
      sourceGroup = in.readBoolean() ? in.readUTF() : null;

      isActive = in.readBoolean();

      created = in.readLong();
      updated = in.readLong();

      hid = in.readLong();
      parentHid = in.readLong();
      variant = in.readLong();

      matchOwner = in.readLong();
      matchProgram = in.readLong();
      matchType = in.readLong();
      matchStamp = in.readLong();

      name = in.readBoolean() ? in.readUTF() : null;
      description = in.readBoolean() ? in.readUTF() : null;
      manufacturer = in.readBoolean() ? in.readUTF() : null;
      manufacturerPrdId = in.readBoolean() ? in.readUTF() : null;
      promoText = in.readBoolean() ? in.readUTF() : null;

      price = in.readDouble();
      oldPrice = in.readBoolean() ? in.readDouble() : null;

      code = in.readBoolean() ? in.readUTF() : null;
      codeType = in.readInt();

      deeplink = in.readBoolean() ? in.readUTF() : null;

      deliveryTime = in.readBoolean() ? in.readUTF() : null;
      deliveryTimeColor = in.readInt();
      deliveryCost = in.readBoolean() ? in.readUTF() : null;
      deliveryCostValue = in.readBoolean() ? in.readDouble() : null;

      imageView = in.readBoolean() ? in.readUTF() : null;
      imageSmall = in.readBoolean() ? in.readUTF() : null;


   }

   public void writeExternal( ObjectOutput out ) throws IOException {
      out.writeLong(contentId);
      out.writeLong(ownerId);
      out.writeBoolean(sourceId != null);
      if ( sourceId != null ) {
         out.writeUTF(sourceId);
      }
      out.writeBoolean(sourceGroup != null);
      if ( sourceGroup != null ) {
         out.writeUTF(sourceGroup);
      }

      out.writeBoolean(isActive);

      out.writeLong(created);
      out.writeLong(updated);

      out.writeLong(hid);
      out.writeLong(parentHid);
      out.writeLong(variant);

      out.writeLong(matchOwner);
      out.writeLong(matchProgram);
      out.writeLong(matchType);
      out.writeLong(matchStamp);

      out.writeBoolean(name != null);
      if ( name != null ) {
         out.writeUTF(name);
      }
      out.writeBoolean(description != null);
      if ( description != null ) {
         out.writeUTF(description);
      }
      out.writeBoolean(manufacturer != null);
      if ( manufacturer != null ) {
         out.writeUTF(manufacturer);
      }
      out.writeBoolean(manufacturerPrdId != null);
      if ( manufacturerPrdId != null ) {
         out.writeUTF(manufacturerPrdId);
      }
      out.writeBoolean(promoText != null);
      if ( promoText != null ) {
         out.writeUTF(promoText);
      }
      out.writeDouble(price);
      out.writeBoolean(oldPrice != null);
      if ( oldPrice != null ) {
         out.writeDouble(oldPrice);
      }

      out.writeBoolean(code != null);
      if ( code != null ) {
         out.writeUTF(code);
      }
      out.writeInt(codeType);

      out.writeBoolean(deeplink != null);
      if ( deeplink != null ) {
         out.writeUTF(deeplink);
      }

      out.writeBoolean(deliveryTime != null);
      if ( deliveryTime != null ) {
         out.writeUTF(deliveryTime);
      }
      out.writeInt(deliveryTimeColor);
      out.writeBoolean(deliveryCost != null);
      if ( deliveryCost != null ) {
         out.writeUTF(deliveryCost);
      }
      out.writeBoolean(deliveryCostValue != null);
      if ( deliveryCostValue != null ) {
         out.writeDouble(deliveryCostValue);
      }

      out.writeBoolean(imageView != null);
      if ( imageView != null ) {
         out.writeUTF(imageView);
      }
      out.writeBoolean(imageSmall != null);
      if ( imageSmall != null ) {
         out.writeUTF(imageSmall);
      }
   }


}


//--------------------------------------------------
// $Id: DumpOfferBeanExternalizable.java 1471 2009-02-26 11:04:18Z michael.krkoska $
//--------------------------------------------------
// Historie:
//
// $Log$
//--------------------------------------------------
